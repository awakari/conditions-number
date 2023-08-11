package model

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestOp_String(t *testing.T) {
	assert.Equal(t, "Undefined", OpUndefined.String())
	assert.Equal(t, "Gte", OpGte.String())
	assert.Equal(t, "Gt", OpGt.String())
	assert.Equal(t, "Lt", OpLt.String())
	assert.Equal(t, "Lte", OpLte.String())
	assert.Equal(t, "Eq", OpEq.String())
}

func TestOp_Int(t *testing.T) {
	assert.Equal(t, 0, int(OpUndefined))
	assert.Equal(t, 1, int(OpGt))
	assert.Equal(t, 2, int(OpGte))
	assert.Equal(t, 3, int(OpEq))
	assert.Equal(t, 4, int(OpLte))
	assert.Equal(t, 5, int(OpLt))
}
