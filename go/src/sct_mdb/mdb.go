package sct_mdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
  "math"
	"strconv"
)

var exec_f uint8 = 0x1
var exec_s uint8 = 0x0

var dt_int      uint8 = 0
var dt_float    uint8 = 1
var dt_str      uint8 = 2
var dt_date     uint8 = 3
var dt_time     uint8 = 4
var dt_datetime uint8 = 5
var dt_un       uint8 = 6

var ds_sign     uint8 = 0
var ds_usgn     uint8 = 1
var ds_un       uint8 = 2

var db_s1 uint8 = 0
var db_s2 uint8 = 1
var db_s4 uint8 = 2
var db_s8 uint8 = 3
var db_sv uint8 = 4
var db_un uint8 = 5

var TypeInt      uint8 = dt_int         //type for integer
var TypeFloat    uint8 = dt_float       //type for float
var TypeStr      uint8 = dt_str         //type for string
var TypeDate     uint8 = dt_date        //type for date
var TypeTime     uint8 = dt_time        //type for time
var TypeDatetime uint8 = dt_datetime    //type for datetime
var TypeUnknow   uint8 = dt_un          //type for unknow type

var SgnSign     uint8 = ds_sign         //signed flag
var SgnUsgn     uint8 = ds_usgn         //unsigned flag
var SgnUnknow   uint8 = ds_un           //unknow flag

var Size1 uint8 = db_s1                 //size 1
var Size2 uint8 = db_s2                 //size 2
var Size4 uint8 = db_s4                 //size 4
var Size8 uint8 = db_s8                 //size 8
var SizeD uint8 = db_sv                 //size dynamic(for string)
var SizeU uint8 = db_un                 //size unknow

var cnn net.Conn

// connect to remote db's server
// ip, the remote server's ip address(v4)
// port, the remote server's listen port
// name, the user's name which want to login
// passwd, the user's password which want to login
// return value:
//   if login failed return false
//   else return true
func Connect(ip string, port int, name string, passwd string) bool {
	var r bool
	r = false

	c, e := net.Dial("tcp", ip+":"+strconv.Itoa(port))

	if e == nil {
		cnn = c
		bb := bytes.NewBuffer([]byte{})

		var nz, pz uint32
		nz = uint32(len(name))
		pz = uint32(len(passwd))

		binary.Write(bb, binary.LittleEndian, &nz)
		bb.WriteString(name)
		binary.Write(bb, binary.LittleEndian, &pz)
		bb.WriteString(passwd)
		var dz uint32
		dz = uint32(bb.Len())
		hb := bytes.NewBuffer([]byte{})
		binary.Write(hb, binary.LittleEndian, &dz)

		d := make([]byte, hb.Len()+bb.Len())
		copy(d, hb.Bytes()[:])
		copy(d[hb.Len():], bb.Bytes()[:])

		/*
			for _, v := range d {
				fmt.Printf("%x", v)
			}
		*/

		c.Write(d)

		rb := make([]byte, 5)

		for t := 0; t < 5; {
			n, e := c.Read(rb[t:])
			if e != nil {
				fmt.Println("recv with err:", e)
				return r
			}
			t += n
		}
		var rz uint32

		binary.Read(bytes.NewBuffer(rb[0:4]), binary.LittleEndian, &rz)
		if rz != 1 {
			fmt.Println("recv with bad data size!")
		}
		var f uint8
		binary.Read(bytes.NewBuffer(rb[4:5]), binary.LittleEndian, &f)
		if f == exec_s {
			r = true
		} else {
			r = false
		}
		/*
			for _, b := range rb {
				fmt.Printf("%x ", b)
			}
		*/
	}
	return r
}

type val_info struct {
	tp uint8
	sz uint8
	sn uint8
}
type date struct {
	y uint16
	m uint8
	d uint8
}
type time struct {
	h uint8
	m uint8
	s uint8
	n uint32
}
type datetime struct {
	d date
	t time
}
type col_value struct {
	u08 []uint8
	u16 []uint16
	u32 []uint32
	u64 []uint64
	i08 []int8
	i16 []int16
	i32 []int32
	i64 []int64
	flt []float32
	dbl []float64
	str []string
	dat []date
	tim []time
	dtm []datetime
}
type dataset struct {
	nms []string
	vis []val_info
	cvs []col_value
	ids []uint64
  inv bool
}

// print the dataset
// the first row is column's name which include system's id named __id
// the values split by ','
func Print(p *dataset) {
  if p == nil { return }
	ds := *p
  if(len(ds.nms) == 0){ return }
	fmt.Print("__id, ")
	for j := 0; j < len(ds.nms); j = j + 1 {
		if j != 0 {
			fmt.Printf(", ")
		}
		fmt.Printf(ds.nms[j])
	}
	fmt.Print("\n")
	for i := 0; i < len(ds.ids); i = i + 1 {
		if i != 0 {
			fmt.Print("\n")
		}
		for j := 0; j < len(ds.nms); j = j + 1 {
			tp := ds.vis[j].tp
			sz := ds.vis[j].sz
			sn := ds.vis[j].sn
			if j != 0 {
				fmt.Print(", ")
			} else {
				fmt.Printf("%d, ", ds.ids[i])
			}
			if tp == dt_int {
				if sz == db_s1 {
					if sn == ds_sign {
						fmt.Print(ds.cvs[j].i08[i])
					} else {
						fmt.Print(ds.cvs[j].u08[i])
					}
				} else if sz == db_s2 {
					if sn == ds_sign {
						fmt.Print(ds.cvs[j].i16[i])
					} else {
						fmt.Print(ds.cvs[j].u16[i])
					}
				} else if sz == db_s4 {
					if sn == ds_sign {
						fmt.Print(ds.cvs[j].i32[i])
					} else {
						fmt.Print(ds.cvs[j].u32[i])
					}
				} else if sz == db_s8 {
					if sn == ds_sign {
						fmt.Print(ds.cvs[j].i64[i])
					} else {
						fmt.Print(ds.cvs[j].u64[i])
					}
				}
			} else if tp == dt_float {
				if sz == db_s4 {
					fmt.Print(ds.cvs[j].flt[i])
				} else {
					fmt.Print(ds.cvs[j].dbl[i])
				}
			} else if tp == dt_str {
				fmt.Print(ds.cvs[j].str[i])
			} else if tp == dt_date {
				fmt.Printf("%4d-%2d-%2d", ds.cvs[j].dat[i].y, ds.cvs[j].dat[i].m, ds.cvs[j].dat[i].d)
			} else if tp == dt_time {
				fmt.Printf("%2d:%2d:%2d.%9d", ds.cvs[j].tim[i].h, ds.cvs[j].tim[i].m, ds.cvs[j].tim[i].s, ds.cvs[j].tim[i].n)
			} else if tp == dt_datetime {
				fmt.Printf("%4d-%2d-%2d %2d:%2d:%2d.%d", ds.cvs[j].dtm[i].d.y, ds.cvs[j].dtm[i].d.m, ds.cvs[j].dtm[i].d.d, ds.cvs[j].dtm[i].t.h, ds.cvs[j].dtm[i].t.m, ds.cvs[j].dtm[i].t.s, ds.cvs[j].dtm[i].t.n)
			} else {
			}
		}
	}

}

func parse_val(p *dataset, rc uint64, r int, c int, d []byte, i int) int {
	ds := *p
	tp := ds.vis[c].tp
	sz := ds.vis[c].sz
	sn := ds.vis[c].sn
	if tp == dt_int {
		if sz == db_s1 {
			if sn == ds_sign {
				if len(ds.cvs[c].i08) == 0 {
					ds.cvs[c].i08 = make([]int8, rc)
				}
				binary.Read(bytes.NewBuffer(d[i:i+1]), binary.LittleEndian, &ds.cvs[c].i08[r])
			} else {
				if len(ds.cvs[c].u08) == 0 {
					ds.cvs[c].u08 = make([]uint8, rc)
				}
				binary.Read(bytes.NewBuffer(d[i:i+1]), binary.LittleEndian, &ds.cvs[c].u08[r])
			}
			i += 1
		} else if sz == db_s2 {
			if sn == ds_sign {
				if len(ds.cvs[c].i16) == 0 {
					ds.cvs[c].i16 = make([]int16, rc)
				}
				binary.Read(bytes.NewBuffer(d[i:i+2]), binary.LittleEndian, &ds.cvs[c].i16[r])
			} else {
				if len(ds.cvs[c].u16) == 0 {
					ds.cvs[c].u16 = make([]uint16, rc)
				}
				binary.Read(bytes.NewBuffer(d[i:i+2]), binary.LittleEndian, &ds.cvs[c].u16[r])
			}
			i += 2
		} else if sz == db_s4 {
			if sn == ds_sign {
				if len(ds.cvs[c].i32) == 0 {
					ds.cvs[c].i32 = make([]int32, rc)
				}
				binary.Read(bytes.NewBuffer(d[i:i+4]), binary.LittleEndian, &ds.cvs[c].i32[r])
			} else {
				if len(ds.cvs[c].u32) == 0 {
					ds.cvs[c].u32 = make([]uint32, rc)
				}
				binary.Read(bytes.NewBuffer(d[i:i+4]), binary.LittleEndian, &ds.cvs[c].u32[r])
			}
			i += 4
		} else if sz == db_s8 {
			if sn == ds_sign {
				if len(ds.cvs[c].i64) == 0 {
					ds.cvs[c].i64 = make([]int64, rc)
				}
				binary.Read(bytes.NewBuffer(d[i:i+8]), binary.LittleEndian, &ds.cvs[c].i64[r])
			} else {
				if len(ds.cvs[c].u64) == 0 {
					ds.cvs[c].u64 = make([]uint64, rc)
				}
				binary.Read(bytes.NewBuffer(d[i:i+8]), binary.LittleEndian, &ds.cvs[c].u64[r])
			}
			i += 8
		}
	} else if tp == dt_float {
		if sz == db_s4 {
			if len(ds.cvs[c].flt) == 0 {
				ds.cvs[c].flt = make([]float32, rc)
			}
			binary.Read(bytes.NewBuffer(d[i:i+4]), binary.LittleEndian, &ds.cvs[c].flt[r])
			i += 4
		} else {
			if len(ds.cvs[c].dbl) == 0 {
				ds.cvs[c].dbl = make([]float64, rc)
			}
			binary.Read(bytes.NewBuffer(d[i:i+8]), binary.LittleEndian, &ds.cvs[c].dbl[r])
			i += 8
		}
	} else if tp == dt_str {
		if len(ds.cvs[c].str) == 0 {
			ds.cvs[c].str = make([]string, rc)
		}
		var s uint16 = 0
		binary.Read(bytes.NewBuffer(d[i:i+2]), binary.LittleEndian, &s)
		i += 2
		b := make([]byte, s)
		binary.Read(bytes.NewBuffer(d[i:i+int(s)]), binary.LittleEndian, b)
		ds.cvs[c].str[r] = string(b)
		i += int(s)
	} else if tp == dt_date {
		if len(ds.cvs[c].dat) == 0 {
			ds.cvs[c].dat = make([]date, rc)
		}
		binary.Read(bytes.NewBuffer(d[i:i+2]), binary.LittleEndian, &ds.cvs[c].dat[r].y)
		i += 2
		binary.Read(bytes.NewBuffer(d[i:i+1]), binary.LittleEndian, &ds.cvs[c].dat[r].m)
		i += 1
		binary.Read(bytes.NewBuffer(d[i:i+1]), binary.LittleEndian, &ds.cvs[c].dat[r].d)
		i += 1
	} else if tp == dt_time {
		if len(ds.cvs[c].dat) == 0 {
			ds.cvs[c].tim = make([]time, rc)
		}
		binary.Read(bytes.NewBuffer(d[i:i+1]), binary.LittleEndian, &ds.cvs[c].tim[r].h)
		i += 1
		binary.Read(bytes.NewBuffer(d[i:i+1]), binary.LittleEndian, &ds.cvs[c].tim[r].m)
		i += 1
		binary.Read(bytes.NewBuffer(d[i:i+1]), binary.LittleEndian, &ds.cvs[c].tim[r].s)
		i += 1
		binary.Read(bytes.NewBuffer(d[i:i+4]), binary.LittleEndian, &ds.cvs[c].tim[r].n)
		i += 4
	} else if tp == dt_datetime {
		if len(ds.cvs[c].dtm) == 0 {
			ds.cvs[c].dtm = make([]datetime, rc)
		}
		binary.Read(bytes.NewBuffer(d[i:i+2]), binary.LittleEndian, &ds.cvs[c].dtm[r].d.y)
		i += 2
		binary.Read(bytes.NewBuffer(d[i:i+1]), binary.LittleEndian, &ds.cvs[c].dtm[r].d.m)
		i += 1
		binary.Read(bytes.NewBuffer(d[i:i+1]), binary.LittleEndian, &ds.cvs[c].dtm[r].d.d)
		i += 1
		binary.Read(bytes.NewBuffer(d[i:i+1]), binary.LittleEndian, &ds.cvs[c].dtm[r].t.h)
		i += 1
		binary.Read(bytes.NewBuffer(d[i:i+1]), binary.LittleEndian, &ds.cvs[c].dtm[r].t.m)
		i += 1
		binary.Read(bytes.NewBuffer(d[i:i+1]), binary.LittleEndian, &ds.cvs[c].dtm[r].t.s)
		i += 1
		binary.Read(bytes.NewBuffer(d[i:i+4]), binary.LittleEndian, &ds.cvs[c].dtm[r].t.n)
		i += 4
	} else {
	}
	//
	return i
}

// exit the remote server
func Exit() {
	if cnn == nil {
		return
	}
	var sz uint32 = 0xffffffff
	sb := bytes.NewBuffer([]byte{})
	binary.Write(sb, binary.LittleEndian, &sz)

	d := make([]byte, sb.Len())
	copy(d, sb.Bytes()[:])

	cnn.Write(d)
}

// get the index from column's name
// return value
//   if there is not exists the column return -1
//   else return the column's index(from 0)
func IdxOf(p *dataset, name string) int {
	d := *p
	for i := 0; i < len(d.nms); i = i + 1 {
		if d.nms[i] == name {
			return i
		}
	}
	return -1
}

// get the type infomation about the column which index is idx
// return value
//   if there is not exists the column return TypeUnknow
//   else return the column's type(TypeXxx..)
func TypeOf(p *dataset, idx int) uint8 {
	d := *p
  if(idx >= len(d.nms)){ return dt_un }
	return d.vis[idx].tp
}

// get the size infomation about the column which index is idx
// return value
//   if there is not exists the column return SizeUnknow
//   else return the column's size(SizeXxx..)
func SizeOf(p *dataset, idx int) uint8 {
	d := *p
  if(idx >= len(d.nms)){ return db_un }
	return d.vis[idx].sz
}

// get the sign infomation about the column which index is idx
// return value
//   if there is not exists the column return SignUnknow
//   else return the column's size(SignXxx..)
func SignOf(p *dataset, idx int) uint8 {
	d := *p
  if(idx >= len(d.nms)){ return ds_un }
	return d.vis[idx].sn
}

// convert the value which row is r, and column is c to int8
// note, the operation is force, use the Type/Size/SignOf get the correct value's infomation before convertion.
// return value
//   if there is not exists the value return MaxInt8
//   else return the value as int8
func AsInt8(p *dataset, r int, c int) int8 {
	d := *p
  if(c >= len(d.nms)){ return math.MaxInt8 }
  if(r >= len(d.ids)){ return math.MaxInt8 }
	return d.cvs[c].i08[r]
}

// convert the value which row is r, and column is c to int16
// note, the operation is force, use the Type/Size/SignOf get the correct value's infomation before convertion.
// return value
//   if there is not exists the value return MaxInt16
//   else return the value as int16
func AsInt16(p *dataset, r int, c int) int16 {
	d := *p
  if(c >= len(d.nms)){ return math.MaxInt16 }
  if(r >= len(d.ids)){ return math.MaxInt16 }
	return d.cvs[c].i16[r]
}

// convert the value which row is r, and column is c to int32
// note, the operation is force, use the Type/Size/SignOf get the correct value's infomation before convertion.
// return value
//   if there is not exists the value return MaxInt32
//   else return the value as int32
func AsInt32(p *dataset, r int, c int) int32 {
	d := *p
  if(c >= len(d.nms)){ return math.MaxInt32 }
  if(r >= len(d.ids)){ return math.MaxInt32 }
	return d.cvs[c].i32[r]
}

// convert the value which row is r, and column is c to int64
// note, the operation is force, use the Type/Size/SignOf get the correct value's infomation before convertion.
// return value
//   if there is not exists the value return MaxInt64
//   else return the value as int64
func AsInt64(p *dataset, r int, c int) int64 {
	d := *p
  if(c >= len(d.nms)){ return math.MaxInt64 }
  if(r >= len(d.ids)){ return math.MaxInt64 }
	return d.cvs[c].i64[r]
}

// convert the value which row is r, and column is c to uint8
// note, the operation is force, use the Type/Size/SignOf get the correct value's infomation before convertion.
// return value
//   if there is not exists the value return MaxUint8
//   else return the value as uint8
func AsUint8(p *dataset, r int, c int) uint8 {
	d := *p
  if(c >= len(d.nms)){ return math.MaxUint8 }
  if(r >= len(d.ids)){ return math.MaxUint8 }
	return d.cvs[c].u08[r]
}

// convert the value which row is r, and column is c to uint16
// note, the operation is force, use the Type/Size/SignOf get the correct value's infomation before convertion.
// return value
//   if there is not exists the value return MaxUint16
//   else return the value as uint16
func AsUint16(p *dataset, r int, c int) uint16 {
	d := *p
  if(c >= len(d.nms)){ return math.MaxUint16 }
  if(r >= len(d.ids)){ return math.MaxUint16 }
	return d.cvs[c].u16[r]
}

// convert the value which row is r, and column is c to uint32
// note, the operation is force, use the Type/Size/SignOf get the correct value's infomation before convertion.
// return value
//   if there is not exists the value return MaxUint32
//   else return the value as uint32
func AsUint32(p *dataset, r int, c int) uint32 {
	d := *p
  if(c >= len(d.nms)){ return math.MaxUint32 }
  if(r >= len(d.ids)){ return math.MaxUint32 }
	return d.cvs[c].u32[r]
}

// convert the value which row is r, and column is c to uint64
// note, the operation is force, use the Type/Size/SignOf get the correct value's infomation before convertion.
// return value
//   if there is not exists the value return MaxUint64
//   else return the value as uint64
func AsUint64(p *dataset, r int, c int) uint64 {
	d := *p
  if(c >= len(d.nms)){ return math.MaxUint64 }
  if(r >= len(d.ids)){ return math.MaxUint64 }
	return d.cvs[c].u64[r]
}

// convert the value which row is r, and column is c to float32
// note, the operation is force, use the Type/Size/SignOf get the correct value's infomation before convertion.
// return value
//   if there is not exists the value return MaxFloat32
//   else return the value as float32
func AsFloat32(p *dataset, r int, c int) float32 {
	d := *p
  if(c >= len(d.nms)){ return math.MaxFloat32 }
  if(r >= len(d.ids)){ return math.MaxFloat32 }
	return d.cvs[c].flt[r]
}

// convert the value which row is r, and column is c to float64
// note, the operation is force, use the Type/Size/SignOf get the correct value's infomation before convertion.
// return value
//   if there is not exists the value return MaxFloat64
//   else return the value as float64
func AsFloat64(p *dataset, r int, c int) float64 {
	d := *p
  if(c >= len(d.nms)){ return math.MaxFloat64 }
  if(r >= len(d.ids)){ return math.MaxFloat64 }
	return d.cvs[c].dbl[r]
}

// convert the value which row is r, and column is c to string
// note, the operation is force, use the Type/Size/SignOf get the correct value's infomation before convertion.
// return value
//   if there is not exists the value return ""
//   else return the value as string
func AsString(p *dataset, r int, c int) string {
	d := *p
  if(c >= len(d.nms)){ return "" }
  if(r >= len(d.ids)){ return "" }
	return d.cvs[c].str[r]
}

// convert the value which row is r, and column is c to date(year, month, day)
// note, the operation is force, use the Type/Size/SignOf get the correct value's infomation before convertion.
// return value
//   if there is not exists the value return 0, 0, 0
//   else return the value as date(year, month, day)
func AsDate(p *dataset, r int, c int) (uint16, uint8, uint8) {
	d := *p
  if(c >= len(d.nms)){ return 0, 0, 0 }
  if(r >= len(d.ids)){ return 0, 0, 0 }
	return d.cvs[c].dat[r].y, d.cvs[c].dat[r].m, d.cvs[c].dat[r].d
}

// convert the value which row is r, and column is c to time(hour, minute, second, nanosecond)
// note, the operation is force, use the Type/Size/SignOf get the correct value's infomation before convertion.
// return value
//   if there is not exists the value return 0, 0, 0, 0
//   else return the value as time(hour, minute, second, nanosecond)
func AsTime(p *dataset, r int, c int) (uint8, uint8, uint8, uint32) {
	d := *p
  if(c >= len(d.nms)){ return 0, 0, 0, 0 }
  if(r >= len(d.ids)){ return 0, 0, 0, 0 }
	return d.cvs[c].tim[r].h, d.cvs[c].tim[r].m, d.cvs[c].tim[r].s, d.cvs[c].tim[r].n
}

// convert the value which row is r, and column is c to datetime(year, month, day, hour, minute, second, nanosecond)
// note, the operation is force, use the Type/Size/SignOf get the correct value's infomation before convertion.
// return value
//   if there is not exists the value return 0, 0, 0, 0, 0, 0, 0
//   else return the value as time(year, month, day, hour, minute, second, nanosecond)
func AsDatetime(p *dataset, r int, c int) (uint16, uint8, uint8, uint8, uint8, uint8, uint32) {
	d := *p
  if(c >= len(d.nms)){ return 0, 0, 0, 0, 0, 0, 0 }
  if(r >= len(d.ids)){ return 0, 0, 0, 0, 0, 0, 0 }
	return d.cvs[c].dtm[r].d.y, d.cvs[c].dtm[r].d.m, d.cvs[c].dtm[r].d.d, d.cvs[c].dtm[r].t.h,d .cvs[c].dtm[r].t.m, d.cvs[c].dtm[r].t.s, d.cvs[c].dtm[r].t.n
}

// give the dataset is valid
// return value
//   if the p is nil or the dataset is not parsed at all return false
//   else return true
func IsValid(p *dataset) bool {
  if(p == nil){ return false }
  d := *p
  return !d.inv
}

// execute the sql's string to remote db server
// return value
//   dataset executed by server
func ExecSql(sql string) dataset {
	var ds dataset
  ds.inv = true
	if cnn == nil {
		return ds
	}
	var sz uint32
	sz = uint32(len(sql))
	sb := bytes.NewBuffer([]byte{})
	binary.Write(sb, binary.LittleEndian, &sz)

	d := make([]byte, sb.Len()+int(sz))
	copy(d, sb.Bytes()[:])
	copy(d[sb.Len():], []byte(sql))

	cnn.Write(d)
	d = make([]byte, 32*1024)

	var rz int = 0
	//read size
	for rz < 4 {
		n, e := cnn.Read(d[rz:])
		if e != nil {
			fmt.Println("recv with err:", e)
			return ds
		}
		rz += n
	}
	binary.Read(bytes.NewBuffer(d[0:4]), binary.LittleEndian, &sz)
	for rz < int(sz)+4 {
		n, e := cnn.Read(d[rz:])
		if e != nil {
			fmt.Println("recv with err:", e)
			return ds
		}
		rz += n
	}
  ds.inv = false
	//parse
	var i int = 4
	var f uint8 = exec_f
	binary.Read(bytes.NewBuffer(d[i:i+1]), binary.LittleEndian, &f)
	i += 1
	if f == exec_f {
		return ds
	}
	var cc uint64 = 0
	binary.Read(bytes.NewBuffer(d[i:i+8]), binary.LittleEndian, &cc)
	i += 8
	ds.nms = make([]string, cc)
	ds.vis = make([]val_info, cc)
	ds.cvs = make([]col_value, cc)
	ds.ids = make([]uint64, cc)
	for j := 0; j < int(cc); j = j + 1 {
		var nc uint64 = 0
		binary.Read(bytes.NewBuffer(d[i:i+8]), binary.LittleEndian, &nc)
		i += 8
		n := string(d[i : i+int(nc)])
		i += int(nc)
		ds.nms[j] = n
	}

	var rc uint64 = 0
	binary.Read(bytes.NewBuffer(d[i:i+8]), binary.LittleEndian, &rc)
	i += 8

	for j := 0; j < int(cc); j = j + 1 {
		binary.Read(bytes.NewBuffer(d[i:i+1]), binary.LittleEndian, &ds.vis[j].sz)
		i += 1
		binary.Read(bytes.NewBuffer(d[i:i+1]), binary.LittleEndian, &ds.vis[j].sn)
		i += 1
		binary.Read(bytes.NewBuffer(d[i:i+1]), binary.LittleEndian, &ds.vis[j].tp)
		i += 1
	}

	ds.ids = make([]uint64, rc)

	for j := 0; j < int(rc); j = j + 1 {
		binary.Read(bytes.NewBuffer(d[i:i+8]), binary.LittleEndian, &ds.ids[j])
		i += 8
		for k := 0; k < int(cc); k = k + 1 {
			i = parse_val(&ds, rc, j, k, d, i)
		}
	}

	return ds

}
