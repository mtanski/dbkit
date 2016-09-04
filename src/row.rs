
/// Index into table/column row
pub type RowOffset = usize;

/// Sub-range for slicing views and columns.
#[derive(Copy, Clone)]
pub struct RowRange {
    /// Index into table/column
    pub offset: RowOffset,
    /// Count of rows
    pub rows: usize,
}
