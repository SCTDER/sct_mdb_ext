
#include "./tab_dataset.h"

namespace kk{

  namespace db{

    namespace mem{

			string val::pack()const{
				string r;
				r += string((const char*)&t, sizeof(t));
				if(t.t == dt_str){
					return r + string((const char*)&s, sizeof(s)) + string(p, s);
				} else {
					return r + string((const char*)&d, sizeof(d));
				}
			}
			string val::pack_val()const{
        if (t.t == dt_str) {
          return string((const char*)&s, sizeof(s)).append(p, s);
        } else { 
				  if (t.t == dt_int) {
					  if (t.s == db_s1) {           return string((const char*)&d, 1);
					  } else if (t.s == db_s2) {    return string((const char*)&d, 2);
					  } else if (t.s == db_s4) {    return string((const char*)&d, 4);
					  } else {                      return string((const char*)&d, 8); }
				  } else if (t.t == dt_float) {
					  if (t.s == db_s4) {           return string((const char*)&d, 4);
					  } else {                      return string((const char*)&d, 8); }
				  } else if (t.t == dt_date) {    return string((const char*)&d.dt.d, sizeof(d.dt.d)); 
          } else if (t.t == dt_time) {    return string((const char*)&d.dt.t, sizeof(d.dt.t)); 
          } else if (t.t == dt_datetime) {return string((const char*)&d.dt, sizeof(d.dt)); 
          } else {}
        }
        return "";
			}
			string val::pack_vi()const{
				return string((const char*)&t, sizeof(t));
			}

      size_t val::parse_val(const char* b, const size_t& c, const val_info& vi) {
        t = vi; size_t i = 0;
        if (t.t == dt_str) {
          memcpy(&s, b + i, sizeof(s));	i += sizeof(s);
          if (s != 0) {
            p = new char[s];
            memcpy(p, b + i, s); i += s;
          }
        } else { 
				  if (t.t == dt_int) {
					  if (t.s == db_s1) {           memcpy(&d, b, 1); i += 1;
					  } else if (t.s == db_s2) {    memcpy(&d, b, 2); i += 2;
					  } else if (t.s == db_s4) {    memcpy(&d, b, 4); i += 4;
					  } else {                      memcpy(&d, b, 8); i += 8; }
				  } else if (t.t == dt_float) {
					  if (t.s == db_s4) {           memcpy(&d, b, 4); i += 4; 
					  } else {                      memcpy(&d, b, 8); i += 8; }
				  } else if (t.t == dt_date) {    memcpy(&d.dt.d, b, sizeof(d.dt.d)); i+= sizeof(d.dt.d);
          } else if (t.t == dt_time) {    memcpy(&d.dt.t, b, sizeof(d.dt.t)); i+= sizeof(d.dt.t);
          } else if (t.t == dt_datetime) {memcpy(&d.dt, b, sizeof(d.dt)); i+= sizeof(d.dt);
          } else {}
        }
        return i;
      }
      size_t val::parse_vi(const char* b, val_info& vi) {
        size_t i = 0;
        memcpy(&vi, b + i, sizeof(vi)); i += sizeof(vi);
        return i;
      }

			size_t val::parse(const char* b, const size_t& c){
				size_t i = 0;
				if( t.t == dt_str ){ delete [] p; p = nptr; } 
				memcpy(&t, b + i, sizeof(t));	i += sizeof(t);
				if( t.t == dt_str ){
					memcpy(&s, b + i, sizeof(s));	i += sizeof(s);
					if( s != 0 ){
						p = new char[s];
						memcpy(p, b + i, s); i += s;
					}
				} else { 
          memcpy(&d, b + i, sizeof(d));
          i += sizeof(d); 
        }
				return i;
			}



			void dataset::as_string(string& dst)const{
				if( h.empty() ){ return; }
        string hs;
        hs.append((const char*)&h.size(), sizeof(h.size()));
        for (size_t i = 0; i < h.size(); ++i) { 
          const size_t& s = h[i].size();
          hs.append((const char*)&s, sizeof(s)).append(h[i]); 
        }
        dst += hs;
				size_t r = d.size();
				dst.append((const char*)&r, sizeof(r));
        if (r != 0) {
          for (size_t j = 0; j < d[0].cs.size(); ++j) { dst += d[0].cs[j].pack_vi(); }
        }
				for( size_t i = 0; i < d.size(); ++i ){
					dst.append((const char*)&(d[i].id), sizeof(d[i].id));
					for( size_t j = 0; j < d[i].cs.size(); ++j ){ dst += d[i].cs[j].pack_val(); }	
				}		
			}
			void dataset::from_string(const string& s){
        h.clear(); d.clear();
        if(s.empty()){ return; }
				size_t k = 0; size_t r = 0; size_t c = 0;
				memcpy( &c, s.c_str() + k, sizeof(c) ); k += sizeof(c);
        h.resize(c);
        kk::adt::vector<val_info> vis;
        vis.resize(c);
        for (size_t i = 0; i < c; ++i) {
          size_t z = 0;
          memcpy(&z, s.c_str() + k, sizeof(z)); k += sizeof(z);
          h[i].resize(z); memcpy(h[i].data(), s.c_str() + k, z); k += z;
        }
				memcpy( &r, s.c_str() + k, sizeof(r) ); k += sizeof(r);
        if (r != 0) {
          for (size_t j = 0; j < c; ++j) { k += val::parse_vi(s.c_str() + k, vis[j]); }
        }
				for( size_t i = 0; i < r; ++i ){
					row rw; 
					memcpy(&rw.id, s.c_str() + k, sizeof(rw.id)); k += sizeof(rw.id);
					for( size_t j = 0; j < c; ++j ){ 
						val v;
						size_t z = v.parse_val(s.c_str() + k, s.size() - k, vis[j]);
						k += z;
						rw.cs.push_back(v);
					}	
					d.push_back(rw);
				}		
			}


    }
  }
}
