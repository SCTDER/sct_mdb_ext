
namespace kk{
  namespace db{



    namespace mem{


      enum val_type   : u08 { dt_int = 0, dt_float = 1, dt_str = 2, dt_date = 3, dt_time = 4, dt_datetime = 5, dt_un = 6 };
      enum val_signed : u08 { ds_sign = 0, ds_usgn = 1, ds_un = 2 };
      enum val_bsize  : u08 { db_s1 = 0, db_s2 = 1, db_s4 = 2, db_s8 = 3, db_sv = 4, db_un = 5 };

    
  #pragma pack (push,1)
      struct val_info {  //for quick index.
        val_info();

        val_bsize  s;
        val_signed g;
        val_type   t;
      };
  #pragma pack (pop)

    struct val { 
      union sz_val {
        i08     ib1;
        i16     ib2;
        i32     ib4;
        i64     ib8;
        u08     ub1;
        u16     ub2;
        u32     ub4;
        u64     ub8;
        float   fb4;
        double  db8;
        date_time dt;
      };

			string pack()const;
			string pack_val()const;
			string pack_vi()const;
      size_t parse_val(const char* b, const size_t& c, const val_info& vi);
      static size_t parse_vi(const char* b, val_info& vi);

			size_t parse(const char* b, const size_t& c);

      sz_val        d;
      char*         p;
      u16           s;
      val_info      t;
    };

    struct dataset {  //output 
      
      struct row {
        size_t        id; //id
        vector<val>   cs; //col values
      };
      struct col {
				string pack()const;
				size_t parse(const char* p, size_t c);

				vector<size_t>  id; //id(sid or invalid value)
				vector<val>     cs; //col values
			};

			void as_string(string& dst)const;
			void from_string(const string& s);

      vector< row >     d;    //store as every row.
      vector<string>    h;    //store field name.
    };


    }
  }
}
