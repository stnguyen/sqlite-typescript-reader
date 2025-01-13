/**
 * Common types
 */

export type ColumnValue = null | number | string;

export interface WhereClause {
    column: string;
    operator: string;
    value: ColumnValue;
}

