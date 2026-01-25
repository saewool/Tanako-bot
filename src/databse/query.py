"""
Query Builder for Columnar Database
Provides a fluent interface for building and executing queries
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Callable, Union
import re
import operator


class Operator(Enum):
    EQ = '='
    NE = '!='
    LT = '<'
    LE = '<='
    GT = '>'
    GE = '>='
    IN = 'IN'
    NOT_IN = 'NOT IN'
    LIKE = 'LIKE'
    NOT_LIKE = 'NOT LIKE'
    IS_NULL = 'IS NULL'
    IS_NOT_NULL = 'IS NOT NULL'
    BETWEEN = 'BETWEEN'
    CONTAINS = 'CONTAINS'
    STARTS_WITH = 'STARTS WITH'
    ENDS_WITH = 'ENDS WITH'
    REGEX = 'REGEX'


class LogicalOperator(Enum):
    AND = 'AND'
    OR = 'OR'
    NOT = 'NOT'


class OrderDirection(Enum):
    ASC = 'ASC'
    DESC = 'DESC'


@dataclass
class Condition:
    column: str
    operator: Operator
    value: Any = None
    secondary_value: Any = None
    
    def _normalize_for_comparison(self, val1: Any, val2: Any) -> tuple:
        """Normalize values for comparison, handling int/string type mismatches."""
        if val1 is None or val2 is None:
            return val1, val2
        
        if isinstance(val1, (int, float)) and isinstance(val2, str):
            try:
                return val1, int(val2) if '.' not in val2 else float(val2)
            except (ValueError, TypeError):
                return val1, val2
        elif isinstance(val1, str) and isinstance(val2, (int, float)):
            try:
                return int(val1) if '.' not in val1 else float(val1), val2
            except (ValueError, TypeError):
                return val1, val2
        
        return val1, val2
    
    def evaluate(self, row: Dict[str, Any]) -> bool:
        col_value = row.get(self.column)
        
        if self.operator == Operator.IS_NULL:
            return col_value is None
        if self.operator == Operator.IS_NOT_NULL:
            return col_value is not None
        
        if col_value is None:
            return False
        
        normalized_col, normalized_val = self._normalize_for_comparison(col_value, self.value)
        
        if self.operator == Operator.EQ:
            return normalized_col == normalized_val
        elif self.operator == Operator.NE:
            return normalized_col != normalized_val
        elif self.operator == Operator.LT:
            return normalized_col < normalized_val
        elif self.operator == Operator.LE:
            return normalized_col <= normalized_val
        elif self.operator == Operator.GT:
            return normalized_col > normalized_val
        elif self.operator == Operator.GE:
            return normalized_col >= normalized_val
        elif self.operator == Operator.IN:
            return col_value in self.value
        elif self.operator == Operator.NOT_IN:
            return col_value not in self.value
        elif self.operator == Operator.LIKE:
            pattern = self._like_to_regex(self.value)
            return bool(re.match(pattern, str(col_value), re.IGNORECASE))
        elif self.operator == Operator.NOT_LIKE:
            pattern = self._like_to_regex(self.value)
            return not bool(re.match(pattern, str(col_value), re.IGNORECASE))
        elif self.operator == Operator.BETWEEN:
            return self.value <= col_value <= self.secondary_value
        elif self.operator == Operator.CONTAINS:
            return self.value in str(col_value)
        elif self.operator == Operator.STARTS_WITH:
            return str(col_value).startswith(self.value)
        elif self.operator == Operator.ENDS_WITH:
            return str(col_value).endswith(self.value)
        elif self.operator == Operator.REGEX:
            return bool(re.search(self.value, str(col_value)))
        
        return False
    
    def _like_to_regex(self, pattern: str) -> str:
        regex = pattern.replace('%', '.*').replace('_', '.')
        return f'^{regex}$'


@dataclass
class ConditionGroup:
    conditions: List[Union['Condition', 'ConditionGroup']] = field(default_factory=list)
    logical_op: LogicalOperator = LogicalOperator.AND
    negated: bool = False
    
    def add(self, condition: Union['Condition', 'ConditionGroup']):
        self.conditions.append(condition)
    
    def evaluate(self, row: Dict[str, Any]) -> bool:
        if not self.conditions:
            return True
        
        if self.logical_op == LogicalOperator.AND:
            result = all(c.evaluate(row) for c in self.conditions)
        elif self.logical_op == LogicalOperator.OR:
            result = any(c.evaluate(row) for c in self.conditions)
        else:
            result = self.conditions[0].evaluate(row) if self.conditions else True
        
        return not result if self.negated else result


@dataclass
class OrderBy:
    column: str
    direction: OrderDirection = OrderDirection.ASC
    nulls_first: bool = False


@dataclass
class Aggregation:
    column: str
    function: str
    alias: Optional[str] = None


class QueryBuilder:
    def __init__(self, table_name: str):
        self.table_name = table_name
        self._select_columns: List[str] = []
        self._conditions: ConditionGroup = ConditionGroup()
        self._order_by: List[OrderBy] = []
        self._limit: Optional[int] = None
        self._offset: int = 0
        self._group_by: List[str] = []
        self._aggregations: List[Aggregation] = []
        self._distinct: bool = False
        self._current_condition_group: ConditionGroup = self._conditions
    
    def select(self, *columns: str) -> 'QueryBuilder':
        self._select_columns.extend(columns)
        return self
    
    def select_all(self) -> 'QueryBuilder':
        self._select_columns = ['*']
        return self
    
    def distinct(self) -> 'QueryBuilder':
        self._distinct = True
        return self
    
    def where(self, column: str, operator: Union[Operator, str], value: Any = None) -> 'QueryBuilder':
        if isinstance(operator, str):
            operator = Operator(operator)
        
        condition = Condition(column=column, operator=operator, value=value)
        self._current_condition_group.add(condition)
        return self
    
    def where_eq(self, column: str, value: Any) -> 'QueryBuilder':
        return self.where(column, Operator.EQ, value)
    
    def where_ne(self, column: str, value: Any) -> 'QueryBuilder':
        return self.where(column, Operator.NE, value)
    
    def where_lt(self, column: str, value: Any) -> 'QueryBuilder':
        return self.where(column, Operator.LT, value)
    
    def where_le(self, column: str, value: Any) -> 'QueryBuilder':
        return self.where(column, Operator.LE, value)
    
    def where_gt(self, column: str, value: Any) -> 'QueryBuilder':
        return self.where(column, Operator.GT, value)
    
    def where_ge(self, column: str, value: Any) -> 'QueryBuilder':
        return self.where(column, Operator.GE, value)
    
    def where_in(self, column: str, values: List[Any]) -> 'QueryBuilder':
        return self.where(column, Operator.IN, values)
    
    def where_not_in(self, column: str, values: List[Any]) -> 'QueryBuilder':
        return self.where(column, Operator.NOT_IN, values)
    
    def where_like(self, column: str, pattern: str) -> 'QueryBuilder':
        return self.where(column, Operator.LIKE, pattern)
    
    def where_null(self, column: str) -> 'QueryBuilder':
        return self.where(column, Operator.IS_NULL)
    
    def where_not_null(self, column: str) -> 'QueryBuilder':
        return self.where(column, Operator.IS_NOT_NULL)
    
    def where_between(self, column: str, min_value: Any, max_value: Any) -> 'QueryBuilder':
        condition = Condition(
            column=column,
            operator=Operator.BETWEEN,
            value=min_value,
            secondary_value=max_value
        )
        self._current_condition_group.add(condition)
        return self
    
    def where_contains(self, column: str, value: str) -> 'QueryBuilder':
        return self.where(column, Operator.CONTAINS, value)
    
    def where_starts_with(self, column: str, value: str) -> 'QueryBuilder':
        return self.where(column, Operator.STARTS_WITH, value)
    
    def where_ends_with(self, column: str, value: str) -> 'QueryBuilder':
        return self.where(column, Operator.ENDS_WITH, value)
    
    def where_regex(self, column: str, pattern: str) -> 'QueryBuilder':
        return self.where(column, Operator.REGEX, pattern)
    
    def and_where(self, column: str, operator: Union[Operator, str], value: Any = None) -> 'QueryBuilder':
        return self.where(column, operator, value)
    
    def or_where(self, column: str, operator: Union[Operator, str], value: Any = None) -> 'QueryBuilder':
        if isinstance(operator, str):
            operator = Operator(operator)
        
        new_group = ConditionGroup(logical_op=LogicalOperator.OR)
        new_group.conditions = self._conditions.conditions.copy()
        new_group.add(Condition(column=column, operator=operator, value=value))
        self._conditions = new_group
        self._current_condition_group = self._conditions
        return self
    
    def group_start(self, logical_op: LogicalOperator = LogicalOperator.AND) -> 'QueryBuilder':
        new_group = ConditionGroup(logical_op=logical_op)
        self._current_condition_group.add(new_group)
        self._current_condition_group = new_group
        return self
    
    def group_end(self) -> 'QueryBuilder':
        self._current_condition_group = self._conditions
        return self
    
    def order_by(self, column: str, direction: OrderDirection = OrderDirection.ASC, nulls_first: bool = False) -> 'QueryBuilder':
        self._order_by.append(OrderBy(column=column, direction=direction, nulls_first=nulls_first))
        return self
    
    def order_by_asc(self, column: str) -> 'QueryBuilder':
        return self.order_by(column, OrderDirection.ASC)
    
    def order_by_desc(self, column: str) -> 'QueryBuilder':
        return self.order_by(column, OrderDirection.DESC)
    
    def limit(self, count: int) -> 'QueryBuilder':
        self._limit = count
        return self
    
    def offset(self, count: int) -> 'QueryBuilder':
        self._offset = count
        return self
    
    def paginate(self, page: int, per_page: int) -> 'QueryBuilder':
        self._offset = (page - 1) * per_page
        self._limit = per_page
        return self
    
    def group_by(self, *columns: str) -> 'QueryBuilder':
        self._group_by.extend(columns)
        return self
    
    def count(self, column: str = '*', alias: Optional[str] = None) -> 'QueryBuilder':
        self._aggregations.append(Aggregation(column=column, function='COUNT', alias=alias))
        return self
    
    def sum(self, column: str, alias: Optional[str] = None) -> 'QueryBuilder':
        self._aggregations.append(Aggregation(column=column, function='SUM', alias=alias))
        return self
    
    def avg(self, column: str, alias: Optional[str] = None) -> 'QueryBuilder':
        self._aggregations.append(Aggregation(column=column, function='AVG', alias=alias))
        return self
    
    def min(self, column: str, alias: Optional[str] = None) -> 'QueryBuilder':
        self._aggregations.append(Aggregation(column=column, function='MIN', alias=alias))
        return self
    
    def max(self, column: str, alias: Optional[str] = None) -> 'QueryBuilder':
        self._aggregations.append(Aggregation(column=column, function='MAX', alias=alias))
        return self
    
    def execute(self, data: Dict[str, List[Any]], columns: List[str]) -> List[Dict[str, Any]]:
        if not data:
            return []
        
        row_count = len(next(iter(data.values())))
        rows = []
        for i in range(row_count):
            row = {col: data[col][i] for col in data}
            rows.append(row)
        
        filtered_rows = [row for row in rows if self._conditions.evaluate(row)]
        
        if self._aggregations:
            return self._execute_aggregation(filtered_rows)
        
        if self._order_by:
            filtered_rows = self._apply_ordering(filtered_rows)
        
        if self._offset > 0:
            filtered_rows = filtered_rows[self._offset:]
        
        if self._limit is not None:
            filtered_rows = filtered_rows[:self._limit]
        
        if self._select_columns and self._select_columns != ['*']:
            filtered_rows = [
                {col: row.get(col) for col in self._select_columns}
                for row in filtered_rows
            ]
        
        if self._distinct:
            seen = set()
            unique_rows = []
            for row in filtered_rows:
                row_key = tuple(sorted(row.items()))
                if row_key not in seen:
                    seen.add(row_key)
                    unique_rows.append(row)
            filtered_rows = unique_rows
        
        return filtered_rows
    
    def _apply_ordering(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        def make_sort_key(row: Dict[str, Any]) -> Tuple:
            keys = []
            for ob in self._order_by:
                value = row.get(ob.column)
                if value is None:
                    sort_value = (0 if ob.nulls_first else 2, None)
                else:
                    sort_value = (1, value)
                keys.append(sort_value)
            return tuple(keys)
        
        reverse_list = [ob.direction == OrderDirection.DESC for ob in self._order_by]
        
        if len(self._order_by) == 1:
            rows.sort(key=make_sort_key, reverse=reverse_list[0])
        else:
            rows.sort(key=make_sort_key)
            if any(reverse_list):
                pass
        
        return rows
    
    def _execute_aggregation(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if self._group_by:
            groups: Dict[Tuple, List[Dict]] = {}
            for row in rows:
                key = tuple(row.get(col) for col in self._group_by)
                if key not in groups:
                    groups[key] = []
                groups[key].append(row)
            
            results = []
            for group_key, group_rows in groups.items():
                result = dict(zip(self._group_by, group_key))
                for agg in self._aggregations:
                    result[agg.alias or f"{agg.function}_{agg.column}"] = self._compute_aggregation(agg, group_rows)
                results.append(result)
            return results
        else:
            result = {}
            for agg in self._aggregations:
                result[agg.alias or f"{agg.function}_{agg.column}"] = self._compute_aggregation(agg, rows)
            return [result] if result else []
    
    def _compute_aggregation(self, agg: Aggregation, rows: List[Dict[str, Any]]) -> Any:
        if agg.function == 'COUNT':
            if agg.column == '*':
                return len(rows)
            return sum(1 for row in rows if row.get(agg.column) is not None)
        
        values = [row.get(agg.column) for row in rows if row.get(agg.column) is not None]
        
        if not values:
            return None
        
        if agg.function == 'SUM':
            return sum(values)
        elif agg.function == 'AVG':
            return sum(values) / len(values)
        elif agg.function == 'MIN':
            return min(values)
        elif agg.function == 'MAX':
            return max(values)
        
        return None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'table': self.table_name,
            'select': self._select_columns,
            'distinct': self._distinct,
            'order_by': [(ob.column, ob.direction.value) for ob in self._order_by],
            'limit': self._limit,
            'offset': self._offset,
            'group_by': self._group_by
        }


def query(table_name: str) -> QueryBuilder:
    return QueryBuilder(table_name)
