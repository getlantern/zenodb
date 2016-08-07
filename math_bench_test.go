package zenodb

import (
	"encoding/binary"
	"math"
	"testing"
)

func BenchmarkMathFloatBigEndian(b *testing.B) {
	buf := make([]byte, 8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f := math.Float64frombits(binary.BigEndian.Uint64(buf))
		binary.BigEndian.PutUint64(buf, math.Float64bits(f+1))
	}
}

func BenchmarkMathFloatLittleEndian(b *testing.B) {
	buf := make([]byte, 8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f := math.Float64frombits(binary.LittleEndian.Uint64(buf))
		binary.LittleEndian.PutUint64(buf, math.Float64bits(f+1))
	}
}

func BenchmarkMathIntBigEndian(b *testing.B) {
	buf := make([]byte, 8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f := int64(binary.BigEndian.Uint64(buf))
		binary.BigEndian.PutUint64(buf, uint64(f+1))
	}
}

func BenchmarkMathIntLittleEndian(b *testing.B) {
	buf := make([]byte, 8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f := int64(binary.LittleEndian.Uint64(buf))
		binary.LittleEndian.PutUint64(buf, uint64(f+1))
	}
}

func BenchmarkMathUintBigEndian(b *testing.B) {
	buf := make([]byte, 8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f := binary.BigEndian.Uint64(buf)
		binary.BigEndian.PutUint64(buf, f+1)
	}
}

func BenchmarkMathUintLittleEndian(b *testing.B) {
	buf := make([]byte, 8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f := binary.LittleEndian.Uint64(buf)
		binary.LittleEndian.PutUint64(buf, f+1)
	}
}
