package main

import (
	"fmt"
  "strconv"
	. "sct_mdb"
	//. "./sct_mdb"
)

func main() {

	if Connect("127.0.0.1", 55551, "user", "user") == true {
		fmt.Println("login successful!")

    var sql string = "create table test_tab_by_go_sdk(id u32(true), id1 u08(true), id2 u16(true)" +
      "  , id3 u32(true), id4 u64(true), id5 i08(true), id6 i16(true), id7 i32(true)" +
      "  , id8 i64(true), v1 float(true), v2 double(true)" +
      "  , s string(true)" +
      "  , d date(true)" +
      "  , t time(true)" +
      "  , dt datetime(true)" + ");"

    d := ExecSql(sql)
    if(IsValid(&d)){ 
      Print(&d) 
    } else { 
      fmt.Println("create test_tab_by_go_sdk failed\n")
      return
    }
    for i := 0; i < 100; i++{
      var f float32 = float32(i)
      var v float64 = float64(f * f)
      fs := strconv.FormatFloat(float64(f), 'f', -1, 32)
      ds := strconv.FormatFloat(v, 'f', -1, 64)
      is := strconv.Itoa(i)
      sql = "insert into test_tab_by_go_sdk ( d, dt, id, id1, id2, id3, id4, id5, id6, id7, id8, s, t, v1, v2 ) values ( \"2019-04-23\", \"2019-04-23 10:11:12.000\", "
      sql = sql + is + ", 1, 2, 3, 4, -1, -2, -3, -4, \"abc\", \"18:00:00.000\", "
      sql = sql + fs + ", " + ds + " );"
      d = ExecSql(sql)
    }
    sql = "select * from test_tab_by_go_sdk"
    d = ExecSql(sql)
    if(IsValid(&d)){ Print(&d) }
    {
      var i int = IdxOf(&d, "id")
      fmt.Println(AsUint32(&d, 10, i))
    }
    {
      var i int = IdxOf(&d, "id4")
      fmt.Println(AsUint64(&d, 10, i))
    }
    {
      var i int = IdxOf(&d, "id5")
      fmt.Println(AsInt8(&d, 10, i))
    }
    {
      var i int = IdxOf(&d, "d")
      fmt.Println(AsDate(&d, 10, i))
    }
    {
      var i int = IdxOf(&d, "dt")
      fmt.Println(AsDatetime(&d, 10, i))
    }
    {
      var i int = IdxOf(&d, "s")
      fmt.Println(AsString(&d, 10, i))
    }
    {
      var i int = IdxOf(&d, "t")
      fmt.Println(AsTime(&d, 10, i))
    }
    {
      var i int = IdxOf(&d, "v2")
      fmt.Println(AsFloat64(&d, 10, i))
    }

    sql = "fields_of test_tab_by_go_sdk"
    d = ExecSql(sql)
    if(IsValid(&d)){ Print(&d); }
    sql = "truncate test_tab_by_go_sdk"
    d = ExecSql(sql)
    sql = "drop table test_tab_by_go_sdk"
    d = ExecSql(sql)
    Print(&d)
    
    Exit()
	} else {
		fmt.Println("login failed!")
	}

	
}
