package rpc

import (
	"gopkg.in/vmihailenco/msgpack.v2"

	"github.com/getlantern/tibsdb/expr"
)

func init() {
	// Register all expressions as extension types so that MsgPack knows how to
	// handle them.
	msgpack.RegisterExt(1, expr.CONST(1))
	msgpack.RegisterExt(2, expr.FIELD("a"))
	msgpack.RegisterExt(3, expr.SUM(1))
	msgpack.RegisterExt(4, expr.COUNT(1))
	msgpack.RegisterExt(5, expr.AVG(1))
	msgpack.RegisterExt(6, expr.LT(1, 2))
	msgpack.RegisterExt(7, expr.LTE(1, 2))
	msgpack.RegisterExt(8, expr.EQ(1, 2))
	msgpack.RegisterExt(9, expr.NEQ(1, 2))
	msgpack.RegisterExt(10, expr.FUZZY_EQ(1, 2, 3))
	msgpack.RegisterExt(11, expr.GTE(1, 2))
	msgpack.RegisterExt(12, expr.GT(1, 2))
	msgpack.RegisterExt(13, expr.AND(1, 2))
	msgpack.RegisterExt(14, expr.OR(1, 2))
	msgpack.RegisterExt(15, expr.ADD(1, 2))
	msgpack.RegisterExt(16, expr.SUB(1, 2))
	msgpack.RegisterExt(17, expr.MULT(1, 2))
	msgpack.RegisterExt(18, expr.DIV(1, 2))
}

type MsgPackCodec struct {
}

func (c *MsgPackCodec) Marshal(v interface{}) ([]byte, error) {
	return msgpack.Marshal(v)
}

func (c *MsgPackCodec) Unmarshal(data []byte, v interface{}) error {
	return msgpack.Unmarshal(data, v)
}

func (c *MsgPackCodec) String() string {
	return "MsgPackCodec"
}
