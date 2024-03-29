// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: dbwire.proto

package database

import (
	bytes "bytes"
	fmt "fmt"
	proto "github.com/gogo/protobuf/proto"
	io "io"
	math "math"
	math_bits "math/bits"
	reflect "reflect"
	strings "strings"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.GoGoProtoPackageIsVersion3 // please upgrade the proto package

type Record struct {
	Flags   uint32 `protobuf:"varint,1,opt,name=flags,proto3" json:"flags,omitempty"`
	Shard   uint32 `protobuf:"varint,2,opt,name=shard,proto3" json:"shard,omitempty"`
	Version uint64 `protobuf:"varint,3,opt,name=version,proto3" json:"version,omitempty"`
	Expire  uint64 `protobuf:"varint,4,opt,name=expire,proto3" json:"expire,omitempty"`
	Value   []byte `protobuf:"bytes,5,opt,name=value,proto3" json:"value,omitempty"`
}

func (m *Record) Reset()      { *m = Record{} }
func (*Record) ProtoMessage() {}
func (*Record) Descriptor() ([]byte, []int) {
	return fileDescriptor_1dc27bdffea2d374, []int{0}
}
func (m *Record) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *Record) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_Record.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *Record) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Record.Merge(m, src)
}
func (m *Record) XXX_Size() int {
	return m.Size()
}
func (m *Record) XXX_DiscardUnknown() {
	xxx_messageInfo_Record.DiscardUnknown(m)
}

var xxx_messageInfo_Record proto.InternalMessageInfo

func (m *Record) GetFlags() uint32 {
	if m != nil {
		return m.Flags
	}
	return 0
}

func (m *Record) GetShard() uint32 {
	if m != nil {
		return m.Shard
	}
	return 0
}

func (m *Record) GetVersion() uint64 {
	if m != nil {
		return m.Version
	}
	return 0
}

func (m *Record) GetExpire() uint64 {
	if m != nil {
		return m.Expire
	}
	return 0
}

func (m *Record) GetValue() []byte {
	if m != nil {
		return m.Value
	}
	return nil
}

func init() {
	proto.RegisterType((*Record)(nil), "database.record")
}

func init() { proto.RegisterFile("dbwire.proto", fileDescriptor_1dc27bdffea2d374) }

var fileDescriptor_1dc27bdffea2d374 = []byte{
	// 202 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0xe2, 0x49, 0x49, 0x2a, 0xcf,
	0x2c, 0x4a, 0xd5, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x17, 0xe2, 0x48, 0x49, 0x2c, 0x49, 0x4c, 0x4a,
	0x2c, 0x4e, 0x55, 0xaa, 0xe1, 0x62, 0x2b, 0x4a, 0x4d, 0xce, 0x2f, 0x4a, 0x11, 0x12, 0xe1, 0x62,
	0x4d, 0xcb, 0x49, 0x4c, 0x2f, 0x96, 0x60, 0x54, 0x60, 0xd4, 0xe0, 0x0d, 0x82, 0x70, 0x40, 0xa2,
	0xc5, 0x19, 0x89, 0x45, 0x29, 0x12, 0x4c, 0x10, 0x51, 0x30, 0x47, 0x48, 0x82, 0x8b, 0xbd, 0x2c,
	0xb5, 0xa8, 0x38, 0x33, 0x3f, 0x4f, 0x82, 0x59, 0x81, 0x51, 0x83, 0x25, 0x08, 0xc6, 0x15, 0x12,
	0xe3, 0x62, 0x4b, 0xad, 0x28, 0xc8, 0x2c, 0x4a, 0x95, 0x60, 0x01, 0x4b, 0x40, 0x79, 0x20, 0x73,
	0xca, 0x12, 0x73, 0x4a, 0x53, 0x25, 0x58, 0x15, 0x18, 0x35, 0x78, 0x82, 0x20, 0x1c, 0x27, 0x93,
	0x0b, 0x0f, 0xe5, 0x18, 0x6e, 0x3c, 0x94, 0x63, 0xf8, 0xf0, 0x50, 0x8e, 0xb1, 0xe1, 0x91, 0x1c,
	0xe3, 0x8a, 0x47, 0x72, 0x8c, 0x27, 0x1e, 0xc9, 0x31, 0x5e, 0x78, 0x24, 0xc7, 0xf8, 0xe0, 0x91,
	0x1c, 0xe3, 0x8b, 0x47, 0x72, 0x0c, 0x1f, 0x1e, 0xc9, 0x31, 0x4e, 0x78, 0x2c, 0xc7, 0x70, 0xe1,
	0xb1, 0x1c, 0xc3, 0x8d, 0xc7, 0x72, 0x0c, 0x49, 0x6c, 0x60, 0x4f, 0x18, 0x03, 0x02, 0x00, 0x00,
	0xff, 0xff, 0x46, 0xbf, 0xcf, 0x18, 0xd4, 0x00, 0x00, 0x00,
}

func (this *Record) Equal(that interface{}) bool {
	if that == nil {
		return this == nil
	}

	that1, ok := that.(*Record)
	if !ok {
		that2, ok := that.(Record)
		if ok {
			that1 = &that2
		} else {
			return false
		}
	}
	if that1 == nil {
		return this == nil
	} else if this == nil {
		return false
	}
	if this.Flags != that1.Flags {
		return false
	}
	if this.Shard != that1.Shard {
		return false
	}
	if this.Version != that1.Version {
		return false
	}
	if this.Expire != that1.Expire {
		return false
	}
	if !bytes.Equal(this.Value, that1.Value) {
		return false
	}
	return true
}
func (this *Record) GoString() string {
	if this == nil {
		return "nil"
	}
	s := make([]string, 0, 9)
	s = append(s, "&database.Record{")
	s = append(s, "Flags: "+fmt.Sprintf("%#v", this.Flags)+",\n")
	s = append(s, "Shard: "+fmt.Sprintf("%#v", this.Shard)+",\n")
	s = append(s, "Version: "+fmt.Sprintf("%#v", this.Version)+",\n")
	s = append(s, "Expire: "+fmt.Sprintf("%#v", this.Expire)+",\n")
	s = append(s, "Value: "+fmt.Sprintf("%#v", this.Value)+",\n")
	s = append(s, "}")
	return strings.Join(s, "")
}
func valueToGoStringDbwire(v interface{}, typ string) string {
	rv := reflect.ValueOf(v)
	if rv.IsNil() {
		return "nil"
	}
	pv := reflect.Indirect(rv).Interface()
	return fmt.Sprintf("func(v %v) *%v { return &v } ( %#v )", typ, typ, pv)
}
func (m *Record) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *Record) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *Record) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Value) > 0 {
		i -= len(m.Value)
		copy(dAtA[i:], m.Value)
		i = encodeVarintDbwire(dAtA, i, uint64(len(m.Value)))
		i--
		dAtA[i] = 0x2a
	}
	if m.Expire != 0 {
		i = encodeVarintDbwire(dAtA, i, uint64(m.Expire))
		i--
		dAtA[i] = 0x20
	}
	if m.Version != 0 {
		i = encodeVarintDbwire(dAtA, i, uint64(m.Version))
		i--
		dAtA[i] = 0x18
	}
	if m.Shard != 0 {
		i = encodeVarintDbwire(dAtA, i, uint64(m.Shard))
		i--
		dAtA[i] = 0x10
	}
	if m.Flags != 0 {
		i = encodeVarintDbwire(dAtA, i, uint64(m.Flags))
		i--
		dAtA[i] = 0x8
	}
	return len(dAtA) - i, nil
}

func encodeVarintDbwire(dAtA []byte, offset int, v uint64) int {
	offset -= sovDbwire(v)
	base := offset
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return base
}
func (m *Record) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if m.Flags != 0 {
		n += 1 + sovDbwire(uint64(m.Flags))
	}
	if m.Shard != 0 {
		n += 1 + sovDbwire(uint64(m.Shard))
	}
	if m.Version != 0 {
		n += 1 + sovDbwire(uint64(m.Version))
	}
	if m.Expire != 0 {
		n += 1 + sovDbwire(uint64(m.Expire))
	}
	l = len(m.Value)
	if l > 0 {
		n += 1 + l + sovDbwire(uint64(l))
	}
	return n
}

func sovDbwire(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}
func sozDbwire(x uint64) (n int) {
	return sovDbwire(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (this *Record) String() string {
	if this == nil {
		return "nil"
	}
	s := strings.Join([]string{`&Record{`,
		`Flags:` + fmt.Sprintf("%v", this.Flags) + `,`,
		`Shard:` + fmt.Sprintf("%v", this.Shard) + `,`,
		`Version:` + fmt.Sprintf("%v", this.Version) + `,`,
		`Expire:` + fmt.Sprintf("%v", this.Expire) + `,`,
		`Value:` + fmt.Sprintf("%v", this.Value) + `,`,
		`}`,
	}, "")
	return s
}
func valueToStringDbwire(v interface{}) string {
	rv := reflect.ValueOf(v)
	if rv.IsNil() {
		return "nil"
	}
	pv := reflect.Indirect(rv).Interface()
	return fmt.Sprintf("*%v", pv)
}
func (m *Record) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowDbwire
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: record: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: record: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Flags", wireType)
			}
			m.Flags = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowDbwire
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Flags |= uint32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Shard", wireType)
			}
			m.Shard = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowDbwire
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Shard |= uint32(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 3:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Version", wireType)
			}
			m.Version = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowDbwire
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Version |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 4:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Expire", wireType)
			}
			m.Expire = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowDbwire
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Expire |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 5:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Value", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowDbwire
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthDbwire
			}
			postIndex := iNdEx + byteLen
			if postIndex < 0 {
				return ErrInvalidLengthDbwire
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Value = append(m.Value[:0], dAtA[iNdEx:postIndex]...)
			if m.Value == nil {
				m.Value = []byte{}
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipDbwire(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthDbwire
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipDbwire(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	depth := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowDbwire
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowDbwire
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
		case 1:
			iNdEx += 8
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowDbwire
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if length < 0 {
				return 0, ErrInvalidLengthDbwire
			}
			iNdEx += length
		case 3:
			depth++
		case 4:
			if depth == 0 {
				return 0, ErrUnexpectedEndOfGroupDbwire
			}
			depth--
		case 5:
			iNdEx += 4
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
		if iNdEx < 0 {
			return 0, ErrInvalidLengthDbwire
		}
		if depth == 0 {
			return iNdEx, nil
		}
	}
	return 0, io.ErrUnexpectedEOF
}

var (
	ErrInvalidLengthDbwire        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowDbwire          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroupDbwire = fmt.Errorf("proto: unexpected end of group")
)
