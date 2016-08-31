
/// Index into table/column row
pub type RowOffset = usize;

#[derive(Copy, Clone)]
pub struct RowRange {
    /// Index into table/column
    pub offset: RowOffset,
    /// Count of rows
    pub rows: usize,
}
