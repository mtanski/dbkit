// vim: set ts=4 sw=4 et :
#[link(name = "ffi")] extern {}

extern crate llvm;
extern crate llvm_sys;
extern crate cbox;

use std::ffi::{CStr, CString};
use std::ops::Deref;
use std::os::raw::{c_uint, c_char};
use std::ptr;

use ::error::DBError;

use self::cbox::{CBox};
use self::llvm::{Context, Module, ExecutionEngine, JitEngine, JitOptions};
use self::llvm_sys::initialization;
use self::llvm_sys::core::*;
use self::llvm_sys::target::*;
use self::llvm_sys::prelude::LLVMModuleRef;
use self::llvm_sys::execution_engine::*;
use self::llvm_sys::target_machine::*;
use self::llvm_sys::transforms::ipo::*;
use self::llvm_sys::transforms::pass_manager_builder::*;
use self::llvm_sys::transforms::vectorize::*;

pub struct JitContext {
    ctx: CBox<Context>,
    module: LLVMModuleRef,
    jit: LLVMExecutionEngineRef,
}

impl JitContext {
    pub fn new() -> Result<JitContext, DBError> {
        let opts = JitOptions { opt_level: 3 };
        unsafe {
            let mut out = JitContext{
                ctx: Context::new(),
                module: ptr::null_mut(),
                jit: ptr::null_mut(),
            };

            out.module = Module::new("", &out.ctx).unwrap();
            out.jit = JitEngine::new(out.module.into(), opts)
                .map(|v| v.unwrap())
                .map_err(|err: CBox<str> | DBError::JITEngine(err.to_string()))?;

            Ok(out)
        }
    }
}

impl Deref for JitContext {
    type Target = JitEngine;
    
    fn deref(&self) -> &Self::Target {
        self.jit.into()
    }
}

impl Drop for JitContext {
    fn drop(&mut self) {
        // TODO: cleanup
    }
}

pub unsafe fn specialize_target(jit: &JitEngine, cpu: &str) -> LLVMTargetMachineRef {
    let cpu_name = CString::new(cpu).unwrap();
    let cpu_features = CString::new("+aes,+avx,+avx2,+bmi,+bmi2,+cmov,+cx16,+f16c,+fma,+fsgsbase,+fxsr,+lzcnt,+mmx,+movbe,+pclmul,+popcnt,+rdrnd,+sse,+sse2,+sse3,+sse4.1,+sse4.2,+ssse3,+xsave,+xsaveopt,-adx,-avx512bw,-avx512cd,-avx512dq,-avx512er,-avx512f,-avx512pf,-avx512vl,-fma4,-hle,-pku,-prfchw,-rdseed,-rtm,-sha,-sse4a,-tbm,-xop,-xsavec,-xsaves").unwrap();

    let default = LLVMGetExecutionEngineTargetMachine(jit.into());
    let triple = LLVMGetTargetMachineTriple(default);
    let target = LLVMGetTargetMachineTarget(default);

    LLVMCreateTargetMachine(target, 
        triple,
        cpu_name.as_ptr(),
        cpu_features.as_ptr(),
        LLVMCodeGenOptLevel::LLVMCodeGenLevelAggressive,
        LLVMRelocMode::LLVMRelocDefault,
        LLVMCodeModel::LLVMCodeModelDefault)
}

pub unsafe fn dump_target(target: LLVMTargetMachineRef) {
    let triple = CStr::from_ptr(LLVMGetTargetMachineTriple(target));
    let cpu = CStr::from_ptr(LLVMGetTargetMachineCPU(target));
    let features = CStr::from_ptr(LLVMGetTargetMachineFeatureString(target));
    println!("triple: {:?}", triple);
    println!("cpu: {:?}", cpu);
    println!("features: {:?}", features);
}

pub unsafe fn force_func_attribute(module: &Module, target: LLVMTargetMachineRef) {
    let triple = CStr::from_ptr(LLVMGetTargetMachineTriple(target));
    let cpu = CStr::from_ptr(LLVMGetTargetMachineCPU(target));
    let features = CStr::from_ptr(LLVMGetTargetMachineFeatureString(target));

    let cpu_key = CString::new("target-cpu").unwrap();
    let features_key = CString::new("target-features").unwrap();

    LLVMSetTarget(module.into(), triple.as_ptr());
    // LLVMSetDataLayout(module.into(), triple.as_ptr());

    // Force optimization arguments into existing functions.
    // This behaves the same ways as LLVM opt
    debug!("Trying to force function args");
    for func in module {
        println!("func: {:?}", cpu);

        if cpu.to_bytes().len() > 0 {
            LLVMAddTargetDependentFunctionAttr(func.into(), cpu_key.as_ptr(), cpu.as_ptr());
        }

        if features.to_bytes().len() > 0 {
            LLVMAddTargetDependentFunctionAttr(func.into(), features_key.as_ptr(), features.as_ptr());
        }
    }
}

pub unsafe fn initilize() {
    let reg = LLVMGetGlobalPassRegistry();
    initialization::LLVMInitializeCore(reg);
    initialization::LLVMInitializeTransformUtils(reg);
    initialization::LLVMInitializeScalarOpts(reg);
    initialization::LLVMInitializeObjCARCOpts(reg);
    initialization::LLVMInitializeVectorization(reg);
    initialization::LLVMInitializeInstCombine(reg);
    initialization::LLVMInitializeIPO(reg);
    initialization::LLVMInitializeAnalysis(reg);
    initialization::LLVMInitializeIPA(reg);
    initialization::LLVMInitializeCodeGen(reg);
    initialization::LLVMInitializeTarget(reg);
}

pub fn optimize_module(module: &Module, jit: &JitEngine, opt: usize, size: usize) -> Result<(), DBError> {

    unsafe {
        let target = specialize_target(jit, "haswell");
        // dump_target(target);

        let builder = LLVMPassManagerBuilderCreate();
        let func_pass_manager = LLVMCreateFunctionPassManagerForModule(module.into());
        let module_pass = LLVMCreatePassManager();

        LLVMPassManagerBuilderSetOptLevel(builder, opt as c_uint);
        LLVMPassManagerBuilderSetSizeLevel(builder, size as c_uint);
        LLVMPassManagerBuilderSetBBVectorize(builder, 1);
        LLVMPassManagerBuilderSetSLPVectorize(builder, 1);
        LLVMPassManagerBuilderSetLoopVectorize(builder, 1);

        if opt > 1 {
            LLVMPassManagerBuilderUseInlinerWithThreshold(builder, size as c_uint);
        } else {
            // otherwise, we will add the builder to the top of the list of passes.
            // This is not exactly what llvm-opt does, but it is pretty close
            LLVMAddAlwaysInlinerPass(module_pass);
        }

        force_func_attribute(module, target);

        LLVMAddAnalysisPasses(target, module_pass);
        LLVMPassManagerBuilderPopulateModulePassManager(builder, module_pass);
        LLVMAddLoopVectorizePass(module_pass);
        LLVMAddSLPVectorizePass(module_pass);

/*
        // Iterate through function and perform function specific optimization
        LLVMAddAnalysisPasses(target, func_pass_manager);
        LLVMAddLoopVectorizePass(func_pass_manager);
        LLVMAddSLPVectorizePass(func_pass_manager);
        LLVMPassManagerBuilderPopulateFunctionPassManager(builder, func_pass_manager);

        if LLVMInitializeFunctionPassManager(func_pass_manager) != 0 {
            return Err(DBError::JITEngine("Function optimizer initialization failed".to_string()))
        }

        for func in module {
            println!("func");
            if LLVMRunFunctionPassManager(func_pass_manager, func.into()) != 0 {
                return Err(DBError::JITEngine("Function optimizer failed".to_string()))
            }
        };

        if LLVMFinalizeFunctionPassManager(func_pass_manager) != 0 {
            return Err(DBError::JITEngine("Function optimizer finalizer failed".to_string()))
        }
*/

        // Perform module wide optimizations
        LLVMRunPassManager(module_pass, module.into());

        LLVMDumpModule(module.into());

        LLVMDisposePassManager(func_pass_manager);
        LLVMDisposePassManager(module_pass);
        LLVMPassManagerBuilderDispose(builder);
    }

    Ok(())
}

#[cfg(test)]
mod tests {

use super::*;
use super::llvm::{Context, Module, ExecutionEngine, JitEngine, JitOptions};

use std::mem;
use std::fs::File;
use std::io::Read;
use std::os::raw::c_uint;

    #[test]
    pub fn compile() {
        unsafe { initilize(); }

        let fname = "data/test/jit/test.ll";
        let mut ctx = JitContext::new().unwrap();

        let mut f = File::open(fname).unwrap();
        let mut buff = String::new();
        f.read_to_string(&mut buff).unwrap();

        let jit: &mut JitEngine = ctx.jit.into();

        let mut unit = Module::parse_ir_from_str(&ctx.ctx, &buff).unwrap();
        let err = unit.verify();
        assert!(err.is_ok(), "Module verify failure: {:?}", err.err());

        optimize_module(&unit, &jit, 3, 0).unwrap();

        jit.add_module(&unit);

        let func: extern "C" fn(c_uint, *const u8, *const u8, *mut u8) -> () = unsafe {
            let func_ref = jit.find_function("bools_and").unwrap();
            let ptr:&u8 = jit.get_global(func_ref);
            mem::transmute(ptr)
        };

        let mut out: [u8;8] = [0;8];
        let rhs: [u8;8] = [0, 1, 0, 1, 0, 1, 0, 1];
        let lhs: [u8;8] = [1, 1, 0, 0, 1, 1, 0, 0];

        func(8, rhs.as_ptr(), lhs.as_ptr(), out.as_mut_ptr());

        assert_eq!(out[0], 0);
        assert_eq!(out[1], 1);
        assert_eq!(out[2], 0);
        assert_eq!(out[3], 0);
        assert_eq!(out[4], 0);
        assert_eq!(out[5], 1);
        assert_eq!(out[6], 0);
        assert_eq!(out[7], 0);
    }
}

