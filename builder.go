package mdatabase

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/uptrace/bun"
)

// JoinType represents the type of SQL JOIN operation
type JoinType int

const (
	InnerJoin JoinType = iota
	LeftJoin
	RightJoin
	FullJoin
)

// String returns the SQL representation of the join type
func (jt JoinType) String() string {
	switch jt {
	case InnerJoin:
		return "INNER JOIN"
	case LeftJoin:
		return "LEFT JOIN"
	case RightJoin:
		return "RIGHT JOIN"
	case FullJoin:
		return "FULL JOIN"
	default:
		return "INNER JOIN"
	}
}

// QueryBuilder provides a fluent, type-safe API for building database queries
type QueryBuilder[T any] struct {
	db        *DB
	ctx       context.Context
	bunQuery  *bun.SelectQuery
	model     *T
	tableName string

	// Query clauses
	selectCols  []string
	joins       []*JoinClause
	wheres      []*WhereClause
	whereGroups []*WhereGroup
	orders      []*OrderClause
	groupBys    []string
	havings     []*WhereClause
	limitVal    *int
	offsetVal   *int

	// Relations to preload
	relations []relationClause

	// Options
	distinct  bool
	forUpdate bool

	// Timeout
	timeout time.Duration

	// Retry configuration
	retryConfig RetryConfig
}

// JoinClause represents a SQL JOIN operation
type JoinClause struct {
	Type       JoinType
	Table      string
	Alias      string
	Conditions []*JoinCondition
}

// JoinCondition represents a condition in a JOIN clause
type JoinCondition struct {
	Left     string
	Operator string
	Right    string
	IsValue  bool // If true, Right is a value; otherwise it's a column
}

// WhereClause represents a WHERE condition
type WhereClause struct {
	Column   string
	Operator string
	Value    any
	IsRaw    bool
	RawSQL   string
	RawArgs  []any
	Negate   bool // For NOT conditions
}

// WhereGroup represents a grouped WHERE condition (for OR/AND grouping)
type WhereGroup struct {
	Conditions []*WhereClause
	Groups     []*WhereGroup
	Connector  string // "AND" or "OR"
	Negate     bool
}

// OrderClause represents an ORDER BY clause
type OrderClause struct {
	Column    string
	Direction string // "ASC" or "DESC"
}

type relationClause struct {
	name  string
	apply func(*bun.SelectQuery) *bun.SelectQuery
}

// OrderDirection represents sort direction
type OrderDirection string

const (
	ASC  OrderDirection = "ASC"
	DESC OrderDirection = "DESC"
)

// JoinBuilder provides a fluent API for building JOIN clauses
type JoinBuilder[T any] struct {
	parent *QueryBuilder[T]
	clause *JoinClause
}

// WhereGroupBuilder provides a fluent API for building grouped WHERE clauses
type WhereGroupBuilder[T any] struct {
	parent *QueryBuilder[T]
	group  *WhereGroup
}

// QueryResult represents the result of a database operation
type QueryResult[T any] struct {
	Data          []T           `json:"data,omitempty"`
	Single        *T            `json:"single,omitempty"`
	Count         int64         `json:"count"`
	Success       bool          `json:"success"`
	Error         error         `json:"error,omitempty"`
	ExecutionTime time.Duration `json:"execution_time"`
	Query         string        `json:"query,omitempty"`
	Args          []any         `json:"args,omitempty"`
}

// Query creates a new QueryBuilder instance
func Query[T any](db *DB) *QueryBuilder[T] {
	return &QueryBuilder[T]{
		db:          db,
		ctx:         context.Background(),
		selectCols:  []string{},
		joins:       []*JoinClause{},
		wheres:      []*WhereClause{},
		whereGroups: []*WhereGroup{},
		orders:      []*OrderClause{},
		groupBys:    []string{},
		havings:     []*WhereClause{},
		relations:   []relationClause{},
		retryConfig: DefaultRetryConfig(),
	}
}

// Context sets the context for the query
func (q *QueryBuilder[T]) Context(ctx context.Context) *QueryBuilder[T] {
	q.ctx = ctx
	return q
}

// Table sets the table name explicitly
func (q *QueryBuilder[T]) Table(name string) *QueryBuilder[T] {
	q.tableName = name
	return q
}

// Select specifies the columns to select
func (q *QueryBuilder[T]) Select(columns ...string) *QueryBuilder[T] {
	q.selectCols = append(q.selectCols, columns...)
	return q
}

// Distinct adds DISTINCT to the query
func (q *QueryBuilder[T]) Distinct() *QueryBuilder[T] {
	q.distinct = true
	return q
}

// Join starts building an INNER JOIN clause
func (q *QueryBuilder[T]) Join(table, alias string) *JoinBuilder[T] {
	clause := &JoinClause{
		Type:       InnerJoin,
		Table:      table,
		Alias:      alias,
		Conditions: []*JoinCondition{},
	}
	return &JoinBuilder[T]{
		parent: q,
		clause: clause,
	}
}

// LeftJoin starts building a LEFT JOIN clause
func (q *QueryBuilder[T]) LeftJoin(table, alias string) *JoinBuilder[T] {
	clause := &JoinClause{
		Type:       LeftJoin,
		Table:      table,
		Alias:      alias,
		Conditions: []*JoinCondition{},
	}
	return &JoinBuilder[T]{
		parent: q,
		clause: clause,
	}
}

// RightJoin starts building a RIGHT JOIN clause
func (q *QueryBuilder[T]) RightJoin(table, alias string) *JoinBuilder[T] {
	clause := &JoinClause{
		Type:       RightJoin,
		Table:      table,
		Alias:      alias,
		Conditions: []*JoinCondition{},
	}
	return &JoinBuilder[T]{
		parent: q,
		clause: clause,
	}
}

// FullJoin starts building a FULL JOIN clause
func (q *QueryBuilder[T]) FullJoin(table, alias string) *JoinBuilder[T] {
	clause := &JoinClause{
		Type:       FullJoin,
		Table:      table,
		Alias:      alias,
		Conditions: []*JoinCondition{},
	}
	return &JoinBuilder[T]{
		parent: q,
		clause: clause,
	}
}

// Where adds a simple WHERE condition (column = value)
func (q *QueryBuilder[T]) Where(column string, value any) *QueryBuilder[T] {
	q.wheres = append(q.wheres, &WhereClause{
		Column:   column,
		Operator: "=",
		Value:    value,
	})
	return q
}

// WhereOp adds a WHERE condition with a custom operator
func (q *QueryBuilder[T]) WhereOp(column, operator string, value any) *QueryBuilder[T] {
	q.wheres = append(q.wheres, &WhereClause{
		Column:   column,
		Operator: operator,
		Value:    value,
	})
	return q
}

// WhereNot adds a WHERE NOT condition
func (q *QueryBuilder[T]) WhereNot(column string, value any) *QueryBuilder[T] {
	q.wheres = append(q.wheres, &WhereClause{
		Column:   column,
		Operator: "=",
		Value:    value,
		Negate:   true,
	})
	return q
}

// WhereIn adds a WHERE IN condition
func (q *QueryBuilder[T]) WhereIn(column string, values []any) *QueryBuilder[T] {
	q.wheres = append(q.wheres, &WhereClause{
		Column:   column,
		Operator: "IN",
		Value:    values,
	})
	return q
}

// WhereNotIn adds a WHERE NOT IN condition
func (q *QueryBuilder[T]) WhereNotIn(column string, values []any) *QueryBuilder[T] {
	q.wheres = append(q.wheres, &WhereClause{
		Column:   column,
		Operator: "IN",
		Value:    values,
		Negate:   true,
	})
	return q
}

// WhereNull adds a WHERE IS NULL condition
func (q *QueryBuilder[T]) WhereNull(column string) *QueryBuilder[T] {
	q.wheres = append(q.wheres, &WhereClause{
		Column:   column,
		Operator: "IS NULL",
	})
	return q
}

// WhereNotNull adds a WHERE IS NOT NULL condition
func (q *QueryBuilder[T]) WhereNotNull(column string) *QueryBuilder[T] {
	q.wheres = append(q.wheres, &WhereClause{
		Column:   column,
		Operator: "IS NOT NULL",
	})
	return q
}

// WhereLike adds a WHERE LIKE condition
func (q *QueryBuilder[T]) WhereLike(column, pattern string) *QueryBuilder[T] {
	q.wheres = append(q.wheres, &WhereClause{
		Column:   column,
		Operator: "LIKE",
		Value:    pattern,
	})
	return q
}

// WhereRaw adds a raw WHERE condition
func (q *QueryBuilder[T]) WhereRaw(sql string, args ...any) *QueryBuilder[T] {
	q.wheres = append(q.wheres, &WhereClause{
		IsRaw:   true,
		RawSQL:  sql,
		RawArgs: args,
	})
	return q
}

// WhereGroup starts building a grouped WHERE clause
func (q *QueryBuilder[T]) WhereGroup(connector string) *WhereGroupBuilder[T] {
	group := &WhereGroup{
		Conditions: []*WhereClause{},
		Groups:     []*WhereGroup{},
		Connector:  connector,
	}
	return &WhereGroupBuilder[T]{
		parent: q,
		group:  group,
	}
}

// Or starts an OR group
func (q *QueryBuilder[T]) Or() *WhereGroupBuilder[T] {
	return q.WhereGroup("OR")
}

// OrderBy adds an ORDER BY clause
func (q *QueryBuilder[T]) OrderBy(column string, direction OrderDirection) *QueryBuilder[T] {
	q.orders = append(q.orders, &OrderClause{
		Column:    column,
		Direction: string(direction),
	})
	return q
}

// GroupBy adds a GROUP BY clause
func (q *QueryBuilder[T]) GroupBy(columns ...string) *QueryBuilder[T] {
	q.groupBys = append(q.groupBys, columns...)
	return q
}

// Having adds a HAVING clause
func (q *QueryBuilder[T]) Having(column, operator string, value any) *QueryBuilder[T] {
	q.havings = append(q.havings, &WhereClause{
		Column:   column,
		Operator: operator,
		Value:    value,
	})
	return q
}

// Limit sets the LIMIT clause
func (q *QueryBuilder[T]) Limit(limit int) *QueryBuilder[T] {
	q.limitVal = &limit
	return q
}

// Offset sets the OFFSET clause
func (q *QueryBuilder[T]) Offset(offset int) *QueryBuilder[T] {
	q.offsetVal = &offset
	return q
}

// Relation specifies a relation to preload (Bun style)
func (q *QueryBuilder[T]) Relation(relation string, apply ...func(*bun.SelectQuery) *bun.SelectQuery) *QueryBuilder[T] {
	var relationApply func(*bun.SelectQuery) *bun.SelectQuery
	if len(apply) > 0 {
		relationApply = apply[0]
	}

	q.relations = append(q.relations, relationClause{
		name:  relation,
		apply: relationApply,
	})
	return q
}

// ForUpdate adds FOR UPDATE clause (for row locking)
func (q *QueryBuilder[T]) ForUpdate() *QueryBuilder[T] {
	q.forUpdate = true
	return q
}

// Timeout sets a timeout for the query
func (q *QueryBuilder[T]) Timeout(duration time.Duration) *QueryBuilder[T] {
	q.timeout = duration
	return q
}

// DisableRetry disables automatic retry for this query
func (q *QueryBuilder[T]) DisableRetry() *QueryBuilder[T] {
	q.retryConfig.EnableRetry = false
	return q
}

// WithRetryConfig sets custom retry configuration
func (q *QueryBuilder[T]) WithRetryConfig(config RetryConfig) *QueryBuilder[T] {
	q.retryConfig = config
	return q
}

// JoinBuilder methods

// On adds a JOIN condition
func (j *JoinBuilder[T]) On(left, operator, right string) *JoinBuilder[T] {
	j.clause.Conditions = append(j.clause.Conditions, &JoinCondition{
		Left:     left,
		Operator: operator,
		Right:    right,
		IsValue:  false,
	})
	return j
}

// OnValue adds a JOIN condition with a value instead of a column
func (j *JoinBuilder[T]) OnValue(left, operator string, value any) *JoinBuilder[T] {
	j.clause.Conditions = append(j.clause.Conditions, &JoinCondition{
		Left:     left,
		Operator: operator,
		Right:    fmt.Sprintf("%v", value),
		IsValue:  true,
	})
	return j
}

// And is an alias for On to make chaining more readable
func (j *JoinBuilder[T]) And(left, operator, right string) *JoinBuilder[T] {
	return j.On(left, operator, right)
}

// End completes the join builder and returns to the query builder
func (j *JoinBuilder[T]) End() *QueryBuilder[T] {
	j.parent.joins = append(j.parent.joins, j.clause)
	return j.parent
}

// WhereGroupBuilder methods

// Where adds a condition to the group
func (w *WhereGroupBuilder[T]) Where(column string, value any) *WhereGroupBuilder[T] {
	w.group.Conditions = append(w.group.Conditions, &WhereClause{
		Column:   column,
		Operator: "=",
		Value:    value,
	})
	return w
}

// WhereOp adds a condition with an operator to the group
func (w *WhereGroupBuilder[T]) WhereOp(column, operator string, value any) *WhereGroupBuilder[T] {
	w.group.Conditions = append(w.group.Conditions, &WhereClause{
		Column:   column,
		Operator: operator,
		Value:    value,
	})
	return w
}

// WhereRaw adds a raw condition to the group
func (w *WhereGroupBuilder[T]) WhereRaw(sql string, args ...any) *WhereGroupBuilder[T] {
	w.group.Conditions = append(w.group.Conditions, &WhereClause{
		IsRaw:   true,
		RawSQL:  sql,
		RawArgs: args,
	})
	return w
}

// End completes the group builder and returns to the query builder
func (w *WhereGroupBuilder[T]) End() *QueryBuilder[T] {
	w.parent.whereGroups = append(w.parent.whereGroups, w.group)
	return w.parent
}

// Helper function to build JOIN SQL
func (j *JoinClause) toSQL() string {
	var sb strings.Builder

	sb.WriteString(j.Type.String())
	sb.WriteString(" ")
	sb.WriteString(j.Table)

	if j.Alias != "" {
		sb.WriteString(" ")
		sb.WriteString(j.Alias)
	}

	if len(j.Conditions) > 0 {
		sb.WriteString(" ON ")
		for i, cond := range j.Conditions {
			if i > 0 {
				sb.WriteString(" AND ")
			}
			sb.WriteString(cond.Left)
			sb.WriteString(" ")
			sb.WriteString(cond.Operator)
			sb.WriteString(" ")
			if cond.IsValue {
				sb.WriteString("?")
			} else {
				sb.WriteString(cond.Right)
			}
		}
	}

	return sb.String()
}

// buildBunQuery builds a Bun SelectQuery from the QueryBuilder
func (q *QueryBuilder[T]) buildBunQuery() *bun.SelectQuery {
	var model T
	return q.buildBunQueryWithModel(&model)
}

// buildBunQueryWithModel builds a Bun SelectQuery with a specific model
func (q *QueryBuilder[T]) buildBunQueryWithModel(model any) *bun.SelectQuery {
	query := q.db.NewSelect().Model(model)

	// Apply table name if specified
	if q.tableName != "" {
		query = query.Table(q.tableName)
	}

	// Apply SELECT columns
	if len(q.selectCols) > 0 {
		query = query.Column(q.selectCols...)
	}

	// Apply DISTINCT
	if q.distinct {
		query = query.Distinct()
	}

	// Apply WHERE conditions
	query = q.applyWhereConditions(query)

	// Apply JOINs
	for _, join := range q.joins {
		query = query.Join(join.toSQL())
	}

	// Apply GROUP BY
	if len(q.groupBys) > 0 {
		query = query.Group(q.groupBys...)
	}

	// Apply HAVING
	for _, having := range q.havings {
		if having.IsRaw {
			query = query.Having(having.RawSQL, having.RawArgs...)
		} else {
			query = query.Having(fmt.Sprintf("%s %s ?", having.Column, having.Operator), having.Value)
		}
	}

	// Apply ORDER BY
	for _, order := range q.orders {
		query = query.Order(fmt.Sprintf("%s %s", order.Column, order.Direction))
	}

	// Apply LIMIT
	if q.limitVal != nil {
		query = query.Limit(*q.limitVal)
	}

	// Apply OFFSET
	if q.offsetVal != nil {
		query = query.Offset(*q.offsetVal)
	}

	// Apply relations (preloading)
	for _, relation := range q.relations {
		if relation.apply != nil {
			query = query.Relation(relation.name, relation.apply)
			continue
		}
		query = query.Relation(relation.name)
	}

	// Apply FOR UPDATE
	if q.forUpdate {
		query = query.For("UPDATE")
	}

	return query
}

// applyWhereConditions applies WHERE conditions to a Bun query
func (q *QueryBuilder[T]) applyWhereConditions(query *bun.SelectQuery) *bun.SelectQuery {
	// Apply simple WHERE conditions
	for _, where := range q.wheres {
		if where.IsRaw {
			query = query.Where(where.RawSQL, where.RawArgs...)
		} else {
			// Handle IN operator specially
			if where.Operator == "IN" {
				if where.Negate {
					query = query.Where("? NOT IN (?)", bun.Ident(where.Column), bun.In(where.Value))
				} else {
					query = query.Where("? IN (?)", bun.Ident(where.Column), bun.In(where.Value))
				}
				continue
			}

			var condition string
			if where.Negate {
				condition = fmt.Sprintf("NOT (%s %s ?)", where.Column, where.Operator)
			} else {
				if where.Operator == "IS NULL" || where.Operator == "IS NOT NULL" {
					condition = fmt.Sprintf("%s %s", where.Column, where.Operator)
					query = query.Where(condition)
					continue
				}
				condition = fmt.Sprintf("%s %s ?", where.Column, where.Operator)
			}
			query = query.Where(condition, where.Value)
		}
	}

	// Apply WHERE groups
	for _, group := range q.whereGroups {
		query = q.applyWhereGroup(query, group)
	}

	return query
}

// applyWhereGroup applies a WHERE group to a Bun query
func (q *QueryBuilder[T]) applyWhereGroup(query *bun.SelectQuery, group *WhereGroup) *bun.SelectQuery {
	if len(group.Conditions) == 0 && len(group.Groups) == 0 {
		return query
	}

	var conditions []string
	var args []any

	// Build conditions
	for _, cond := range group.Conditions {
		if cond.IsRaw {
			conditions = append(conditions, cond.RawSQL)
			args = append(args, cond.RawArgs...)
		} else {
			var condStr string
			if cond.Operator == "IS NULL" || cond.Operator == "IS NOT NULL" {
				condStr = fmt.Sprintf("%s %s", cond.Column, cond.Operator)
			} else {
				condStr = fmt.Sprintf("%s %s ?", cond.Column, cond.Operator)
				args = append(args, cond.Value)
			}
			conditions = append(conditions, condStr)
		}
	}

	// Build group SQL
	if len(conditions) > 0 {
		groupSQL := "(" + joinStrings(conditions, " "+group.Connector+" ") + ")"
		if group.Negate {
			groupSQL = "NOT " + groupSQL
		}
		query = query.Where(groupSQL, args...)
	}

	return query
}

// Helper function to join strings
func joinStrings(strs []string, sep string) string {
	if len(strs) == 0 {
		return ""
	}
	if len(strs) == 1 {
		return strs[0]
	}

	result := strs[0]
	for i := 1; i < len(strs); i++ {
		result += sep + strs[i]
	}
	return result
}
