package model

type Op int

const (
	OpUndefined Op = iota
	OpGt
	OpGte
	OpEq
	OpLte
	OpLt
)

func (op Op) String() string {
	return [...]string{
		"Undefined",
		"Gt",
		"Gte",
		"Eq",
		"Lte",
		"Lt",
	}[op]
}
