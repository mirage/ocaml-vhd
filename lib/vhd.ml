(*
 * Copyright (C) 2011-2013 Citrix Inc
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published
 * by the Free Software Foundation; version 2.1 only. with the special
 * exception on linking described in file LICENSE.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *)


(* VHD manipulation *)

module Feature = struct
  type t = 
    | Temporary

  let of_int x =
    if x land 1 <> 0 then [ Temporary ] else []

  let to_int ts =
    let one = function
      | Temporary -> 1 in
    let reserved = 2 in (* always set *)
    List.fold_left (lor) reserved (List.map one ts)

  let to_string = function
    | Temporary -> "Temporary"
end

module Disk_type = struct
  type t = 
    | None
    | Reserved of int
    | Fixed_hard_disk
    | Dynamic_hard_disk
    | Differencing_hard_disk

  let of_int32 = function
    | 0l -> None
    | 1l -> Reserved 1
    | 2l -> Fixed_hard_disk 
    | 3l -> Dynamic_hard_disk
    | 4l -> Differencing_hard_disk
    | 5l -> Reserved 5
    | 6l -> Reserved 6
    | _ -> failwith "Unhandled disk type!"

  let to_int32 = function
    | None -> 0l
    | Reserved i -> Int32.of_int i
    | Fixed_hard_disk -> 2l
    | Dynamic_hard_disk -> 3l
    | Differencing_hard_disk -> 4l

  let to_string = function
    | None -> "None"
    | Reserved x -> Printf.sprintf "Reserved %d" x
    | Fixed_hard_disk -> "Fixed_hard_disk"
    | Dynamic_hard_disk -> "Dynamic_hard_disk"
    | Differencing_hard_disk -> "Differencing_hard_disk"
end

module Host_OS = struct
  type t =
    | Windows
    | Macintosh
    | Other of string

  let of_bytes = function
    | "\x57\x69\x32\x6b" -> Windows
    | "\x4d\x61\x63\x20" -> Macintosh
    | x -> Other x

  let to_bytes = function
    | Windows -> "\x57\x69\x32\x6b"
    | Macintosh -> "\x4d\x61\x63\x20"
    | Other x -> x

  let to_string = function
    | Windows -> "Windows"
    | Macintosh -> "Macintosh"
    | Other x -> Printf.sprintf "Other [%02x %02x %02x %02x]"
      (int_of_char x.[0]) (int_of_char x.[1]) (int_of_char x.[2]) (int_of_char x.[3])
end

module Geometry = struct
  type t = {
    cylinders : int;
    heads : int;
    sectors : int;
  }

  let to_string t = Printf.sprintf "{ cylinders = %d; heads = %d; sectors = %d }"
    t.cylinders t.heads t.sectors
end

module Footer = struct
  type t = {
    cookie : string;
    features : Feature.t list;
    format_version : int32;
    data_offset : int64;
    time_stamp : int32;
    creator_application : string;
    creator_version : int32;
    creator_host_os : Host_OS.t;
    original_size : int64;
    current_size : int64;
    geometry : Geometry.t;
    disk_type : Disk_type.t;
    checksum : int32;
    uid : string;
    saved_state : bool
  }
end

module Platform_code = struct
  type t =
    | None
    | Wi2r
    | Wi2k
    | W2ru
    | W2ku
    | Mac
    | MacX

  let wi2r = 0x57693272l
  let wi2k = 0x5769326Bl
  let w2ru = 0x57327275l
  let w2ku = 0x57326b75l
  let mac = 0x4d616320l
  let macx = 0x4d616358l

  let of_int32 = function
    | 0l -> None
    | x when x = wi2r -> Wi2r
    | x when x = wi2k -> Wi2k
    | x when x = w2ru -> W2ru
    | x when x = w2ku -> W2ku
    | x when x = mac -> Mac
    | x when x = macx -> MacX
    | x -> failwith (Printf.sprintf "unknown platform_code: %lx" x)

  let to_int32 = function
    | None -> 0l
    | Wi2r -> wi2r
    | Wi2k -> wi2k
    | W2ru -> w2ru
    | W2ku -> w2ku
    | Mac -> mac
    | MacX -> macx

  let to_string = function
    | None -> "None"
    | Wi2r -> "Wi2r [deprecated]"
    | Wi2k -> "Wi2k [deprecated]"
    | W2ru -> "W2ru"
    | W2ku -> "W2ku"
    | Mac  -> "Mac "
    | MacX -> "MacX"
end

module Parent_locator = struct
  type t = {
    platform_code : Platform_code.t;

    (* WARNING WARNING - the following field is measured in *bytes* because Viridian VHDs 
       do this. This is a deviation from the spec. When reading in this field, we multiply
       by 512 if the value is less than 511 *)
    platform_data_space : int32;
    platform_data_space_original : int32; (* Original unaltered value *)

    platform_data_length : int32;
    platform_data_offset : int64;
    platform_data : string;
  }


  let null = {
    platform_code=Platform_code.None;
    platform_data_space=0l;
    platform_data_space_original=0l;
    platform_data_length=0l;
    platform_data_offset=0L;
    platform_data="";
  }

  let to_string t =
    Printf.sprintf "(%s %lx %lx, %ld, 0x%Lx, %s)" (Platform_code.to_string t.platform_code)
      t.platform_data_space t.platform_data_space_original
      t.platform_data_length t.platform_data_offset t.platform_data
end

type vhd_header = {
  h_cookie : string;
  h_data_offset : int64;
  mutable h_table_offset : int64;
  h_header_version : int32;
  mutable h_max_table_entries : int32;
  h_block_size : int32;
  h_checksum : int32;
  h_parent_unique_id : string;
  h_parent_time_stamp : int32;
  h_parent_unicode_name : int array;
  h_parent_locators : Parent_locator.t array;
}

type vhd = {
  filename : string;
  mmap : Lwt_bytes.t;
  header : vhd_header;
  footer : Footer.t;
  parent : vhd option;
  bat : int32 array;
}


let footer_cookie = "conectix"
let header_cookie = "cxsparse"

let sector_size = 512
let sector_sizeL = 512L
let block_size = 0x200000l
let unusedl = 0xffffffffl
let unusedL = 0xffffffffffffffffL

let header_version = 0x00010000l
let footer_version = 0x00010000l

let creator_application = "caml"
let creator_version = 0x00000001l

let y2k = 946684800.0 (* seconds from the unix epoch to the vhd epoch *)

(******************************************************************************)
(* A couple of helper functions                                               *)
(******************************************************************************)
  
(** Guarantee to read 'n' bytes from a file descriptor or raise End_of_file *)
let really_read mmap pos n = 
	let buffer = String.create (Int64.to_int n) in
	let pos2 = Int64.mul (Int64.div n 4096L) 4096L in
(*	Lwt_bytes.madvise mmap (Int64.to_int pos2) (Int64.to_int (Int64.sub (Int64.add n pos) pos2)) Lwt_bytes.MADV_WILLNEED;
	lwt () = Lwt_bytes.wait_mincore mmap (Int64.to_int pos) in*)
    Lwt_bytes.blit_bytes_string mmap (Int64.to_int pos) buffer 0 (Int64.to_int n);
    Lwt.return buffer

(** Guarantee to write the string 'str' to a fd or raise an exception *)
let really_write mmap pos str =
    Lwt.return (Lwt_bytes.blit_string_bytes str 0 mmap (Int64.to_int pos) (String.length str))

(** Turn an array of ints into a utf8 encoded string *)
let utf16_to_string s =
  let utf8_chars_of_int i = 
    if i < 0x80 then [char_of_int i] 
    else if i < 0x800 then 
      begin
	let z = i land 0x3f
	and y = (i lsr 6) land 0x1f in 
	[char_of_int (0xc0 + y); char_of_int (0x80+z)]
      end
    else if i < 0x10000 then
      begin
	let z = i land 0x3f
	and y = (i lsr 6) land 0x3f 
	and x = (i lsr 12) land 0x0f in
	[char_of_int (0xe0 + x); char_of_int (0x80+y); char_of_int (0x80+z)]
      end
    else if i < 0x110000 then
      begin
	let z = i land 0x3f
	and y = (i lsr 6) land 0x3f
	and x = (i lsr 12) land 0x3f
	and w = (i lsr 18) land 0x07 in
	[char_of_int (0xf0 + w); char_of_int (0x80+x); char_of_int (0x80+y); char_of_int (0x80+z)]
      end
    else
      failwith "Bad unicode character!"
  in
  String.concat "" (List.map (fun c -> Printf.sprintf "%c" c) (List.flatten (List.map utf8_chars_of_int (Array.to_list s))))

(** bizarre CHS calculation *)
let get_chs sectors =
  let max_secs = 65535*255*16 in
  let secs = min max_secs sectors in

  let secs_per_track = ref 0 in
  let heads = ref 0 in
  let cyls_times_heads = ref 0 in
  
  if secs > 65535*63*16 then
    begin
      secs_per_track := 255;
      heads := 16;
      cyls_times_heads := secs / !secs_per_track;
    end
  else
    begin
      secs_per_track := 17;
      cyls_times_heads := secs / !secs_per_track;
      
      heads := max ((!cyls_times_heads+1023)/1024) 4;

      if (!cyls_times_heads >= (!heads * 1024) || !heads > 16)
      then
	begin
	  secs_per_track := 31;
	  heads := 16;
	  cyls_times_heads := secs / !secs_per_track;
	end;
      
      if (!cyls_times_heads >= (!heads*1024))
      then
	begin
	  secs_per_track := 63;
	  heads := 16;
	  cyls_times_heads := secs / !secs_per_track;
	end	    
    end;
  (!cyls_times_heads / !heads, !heads, !secs_per_track)

let get_vhd_time time =
  Int32.of_int (int_of_float (time -. y2k))

let get_now () =
  let time = Unix.time() in
  get_vhd_time time

let get_parent_modification_time parent =
  let st = Unix.stat parent in
  get_vhd_time (st.Unix.st_mtime)
    
(* FIXME: This function does not do what it says! *)
let utf16_of_utf8 string =
  Array.init (String.length string) 
    (fun c -> int_of_char string.[c])

(*****************************************************************************)
(* Generic unmarshalling functions                                           *)
(*****************************************************************************)

(* bigendian parameter means the data is stored on disk in bigendian format *)

let unmarshal_uint8 (s, offset) =
  int_of_char s.[offset], (s, offset+1)

let unmarshal_uint16 ?(bigendian=true) (s, offset) =
  let offsets = if bigendian then [|1;0|] else [|0;1|] in
  let ( <|< ) a b = a lsl b 
  and ( || ) a b = a lor b in
  let a = int_of_char (s.[offset + offsets.(0)]) 
  and b = int_of_char (s.[offset + offsets.(1)]) in
  (a <|< 0) || (b <|< 8), (s, offset + 2)

let unmarshal_uint32 ?(bigendian=true) (s, offset) = 
	let offsets = if bigendian then [|3;2;1;0|] else [|0;1;2;3|] in
	let ( <|< ) a b = Int32.shift_left a b 
	and ( || ) a b = Int32.logor a b in
	let a = Int32.of_int (int_of_char (s.[offset + offsets.(0)])) 
	and b = Int32.of_int (int_of_char (s.[offset + offsets.(1)])) 
	and c = Int32.of_int (int_of_char (s.[offset + offsets.(2)])) 
	and d = Int32.of_int (int_of_char (s.[offset + offsets.(3)])) in
	(a <|< 0) || (b <|< 8) || (c <|< 16) || (d <|< 24), (s, offset + 4)
		
let unmarshal_uint64 ?(bigendian=true) (s, offset) = 
  let offsets = if bigendian then [|7;6;5;4;3;2;1;0|] else [|0;1;2;3;4;5;6;7|] in
  let ( <|< ) a b = Int64.shift_left a b 
  and ( || ) a b = Int64.logor a b in
  let a = Int64.of_int (int_of_char (s.[offset + offsets.(0)])) 
  and b = Int64.of_int (int_of_char (s.[offset + offsets.(1)])) 
  and c = Int64.of_int (int_of_char (s.[offset + offsets.(2)])) 
  and d = Int64.of_int (int_of_char (s.[offset + offsets.(3)])) 
  and e = Int64.of_int (int_of_char (s.[offset + offsets.(4)])) 
  and f = Int64.of_int (int_of_char (s.[offset + offsets.(5)])) 
  and g = Int64.of_int (int_of_char (s.[offset + offsets.(6)])) 
  and h = Int64.of_int (int_of_char (s.[offset + offsets.(7)])) in
  (a <|< 0) || (b <|< 8) || (c <|< 16) || (d <|< 24)
  || (e <|< 32) || (f <|< 40) || (g <|< 48) || h <|< (56), (s, offset + 8)

let unmarshal_string len (s,offset) =
  String.sub s offset len, (s, offset + len)

(* Unmarshal a UTF-16 string. Result is an array containing ints representing the chars *)
let unmarshal_utf16_string len (s, offset) =
  (* Check the byte order *)
  let bigendian,pos,max = 
    begin
      let num,pos2 = unmarshal_uint16 (s,offset) in
      match num with 
	| 0xfeff -> true,pos2,(len/2-1)
	| 0xfffe -> false,pos2,(len/2-1)
	| _ -> true,(s,offset),(len/2)
    end
  in

  let string = Array.create max 0 in

  let rec inner n pos =
    if n=max then pos else
      begin
	let c,pos = unmarshal_uint16 ~bigendian pos in
	let code,nextpos,newn = 
	  if c >= 0xd800 && c <= 0xdbff then
	    begin
	      let c2,pos = unmarshal_uint16 ~bigendian pos in
	      if c2 < 0xdc00 || c2 > 0xdfff then (failwith "Bad unicode value!");
	      let top10bits = c-0xd800 in
	      let bottom10bits = c2-0xdc00 in
	      let char = 0x10000 + (bottom10bits lor (top10bits lsl 10)) in
	      char,pos,(n+2)
	    end
	  else
	    c,pos,(n+1)
	in
	string.(n) <- code;
	inner newn nextpos
      end
  in
  let pos = inner 0 pos in
  string, pos

let marshal_int8 x =
  String.make 1 (char_of_int x)

let marshal_int16 ?(bigendian=true) x = 
  let offsets = if bigendian then [|1;0|] else [|0;1|] in
  let (>|>) a b = a lsr b
  and (&&) a b = a land b in
  let a = (x >|> 0) && 0xff 
  and b = (x >|> 8) && 0xff in
  let result = String.make 2 '\000' in
  result.[offsets.(0)] <- char_of_int a;
  result.[offsets.(1)] <- char_of_int b;
  result

let marshal_int32 ?(bigendian=true) x = 
  let offsets = if bigendian then [|3;2;1;0|] else [|0;1;2;3|] in
  let (>|>) a b = Int32.shift_right_logical a b
  and (&&) a b = Int32.logand a b in
  let a = (x >|> 0) && 0xffl 
  and b = (x >|> 8) && 0xffl
  and c = (x >|> 16) && 0xffl
  and d = (x >|> 24) && 0xffl in
  let result = String.make 4 '\000' in
  result.[offsets.(0)] <- char_of_int (Int32.to_int a);
  result.[offsets.(1)] <- char_of_int (Int32.to_int b);
  result.[offsets.(2)] <- char_of_int (Int32.to_int c);
  result.[offsets.(3)] <- char_of_int (Int32.to_int d);
  result

let marshal_int64 ?(bigendian=true) x = 
  let offsets = if bigendian then [|7;6;5;4;3;2;1;0|] else [|0;1;2;3;4;5;6;7|] in
  let (>|>) a b = Int64.shift_right_logical a b
  and (&&) a b = Int64.logand a b in
  let a = (x >|> 0) && 0xffL
  and b = (x >|> 8) && 0xffL
  and c = (x >|> 16) && 0xffL
  and d = (x >|> 24) && 0xffL 
  and e = (x >|> 32) && 0xffL 
  and f = (x >|> 40) && 0xffL
  and g = (x >|> 48) && 0xffL
  and h = (x >|> 56) && 0xffL in
  let result = String.make 8 '\000' in
  result.[offsets.(0)] <- char_of_int (Int64.to_int a);
  result.[offsets.(1)] <- char_of_int (Int64.to_int b);
  result.[offsets.(2)] <- char_of_int (Int64.to_int c);
  result.[offsets.(3)] <- char_of_int (Int64.to_int d);
  result.[offsets.(4)] <- char_of_int (Int64.to_int e);
  result.[offsets.(5)] <- char_of_int (Int64.to_int f);
  result.[offsets.(6)] <- char_of_int (Int64.to_int g);
  result.[offsets.(7)] <- char_of_int (Int64.to_int h);
  result

let marshal_utf16 x =
  let rec inner n cur =
    if n=Array.length x then (String.concat "" (List.rev cur))
    else
      let char = x.(n) in
      if char < 0x10000
      then inner (n+1) ((marshal_int16 char)::cur)
      else
	begin
	  let char = char - 0x10000 in
	  let c1 = (char lsr 10) land 0x3ff in (* high bits *)
	  let c2 = char land 0x3ff in (* low bits *)
	  let str1 = marshal_int16 (0xd800 + c1) 
	  and str2 = marshal_int16 (0xdc00 + c2)
	  in 
	  inner (n+1) ((str1^str2)::cur)	      
	end
  in inner 0 []

let pad_string_to str n =
  let newstr = String.make n '\000' in
  let len = min (String.length str) n in
  String.blit str 0 newstr 0 len;
  newstr

(******************************************************************************)
(* Specific VHD unmarshalling functions                                       *)
(******************************************************************************)

let get_block_sizes vhd =
  let block_size = vhd.header.h_block_size in
  let nsectors = Int32.div block_size 512l in
  let bitmap_size = Int32.div nsectors 8l in
  (block_size, bitmap_size, Int32.add block_size bitmap_size)

let unmarshal_geometry pos =
  let cyl,pos = unmarshal_uint16 pos in
  let heads,pos = unmarshal_uint8 pos in
  let sectors, pos = unmarshal_uint8 pos in
  {Geometry.cylinders = cyl;
   heads; sectors}, pos

let unmarshal_parent_locator mmap pos =
  let platform_code,pos =
    let code, pos = unmarshal_uint32 pos in
    Platform_code.of_int32 code, pos in

  (* WARNING WARNING - see comment on field at the beginning of this file *)
  let platform_data_space_original,platform_data_space,pos = 
    let space,pos = unmarshal_uint32 pos in
      if space < 511l 
      then 
	space,Int32.mul 512l space, pos
      else
	(Printf.printf "WARNING: probable deviation from spec in platform_data_space (got %ld)\n" space; space,space,pos)
  in
  let platform_data_length,pos = unmarshal_uint32 pos in
  let _,pos = unmarshal_uint32 pos in
  let platform_data_offset,pos = unmarshal_uint64 pos in
  lwt platform_data = 
    if platform_data_length > 0l then
      (Printf.printf "Platform_data_length: %ld\n" platform_data_length;
       really_read mmap platform_data_offset (Int64.of_int32 platform_data_length))
    else Lwt.return ""
 in
 Lwt.return ({Parent_locator.platform_code; platform_data_space;
   platform_data_space_original; platform_data_length;platform_data_offset;platform_data
   },pos)

let unmarshal_n n pos f =
  let rec inner m pos cur =
    if m=n then Lwt.return ((List.rev cur),pos) else
      begin
	lwt x,newpos = f pos in
	inner (m+1) newpos (x::cur)
      end
  in inner 0 pos []
  
let unmarshal_parent_locators mmap pos =
  lwt list,pos = unmarshal_n 8 pos (unmarshal_parent_locator mmap) in
  Lwt.return (Array.of_list list, pos)

let parse_bitfield num =
  let rec inner n mask cur =
    if n=64 then cur else
      if Int64.logand num mask = mask 
      then inner (n+1) (Int64.shift_left mask 1) (n::cur)
      else inner (n+1) (Int64.shift_left mask 1) cur
  in inner 0 Int64.one []

let uuidarr_to_string uuid =
  Printf.sprintf "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x"
    uuid.(0) uuid.(1) uuid.(2) uuid.(3) uuid.(4) uuid.(5)
    uuid.(6) uuid.(7) uuid.(8) uuid.(9) uuid.(10) uuid.(11)
    uuid.(12) uuid.(13) uuid.(14) uuid.(15)

let read_footer mmap pos read_512 =
  lwt footer = really_read mmap pos (if read_512 then 512L else 511L) in
  let footer = (footer,0) in
  let cookie,pos = unmarshal_string 8 footer in
  let features,pos = 
    let featuresnum,pos = unmarshal_uint32 pos in
    Feature.of_int (Int32.to_int featuresnum), pos in
  let format_version,pos = unmarshal_uint32 pos in
  let data_offset,pos = unmarshal_uint64 pos in
  let time_stamp,pos = unmarshal_uint32 pos in
  let creator_application,pos = unmarshal_string 4 pos in
  let creator_version,pos = unmarshal_uint32 pos in
  let creator_host_os,pos =
    let s,pos = unmarshal_string 4 pos in
    Host_OS.of_bytes s, pos in
  let original_size,pos = unmarshal_uint64 pos in
  let current_size,pos = unmarshal_uint64 pos in
  let geometry,pos = unmarshal_geometry pos in
  let disk_type,pos = 
    let num,pos = unmarshal_uint32 pos in
    Disk_type.of_int32 num, pos in
  let checksum,pos = unmarshal_uint32 pos in
  lwt list,pos = unmarshal_n 16 pos (fun pos -> Lwt.return (unmarshal_uint8 pos) ) in
  let uid = uuidarr_to_string (Array.of_list list) in
  let saved_state,pos = let n,pos = unmarshal_uint8 pos in n=1, pos in
  let footer_buffer,_ = footer in
  let open Footer in
  Lwt.return {cookie; features; format_version; data_offset; time_stamp; creator_version;
   creator_application; creator_host_os; original_size; current_size; geometry;
   disk_type; checksum; uid; saved_state}

let read_header mmap pos =
  lwt str = really_read mmap pos 1024L in
  let header = (str,0) in
  let h_cookie,pos = unmarshal_string 8 header in
  let h_data_offset,pos = unmarshal_uint64 pos in
  let h_table_offset,pos = unmarshal_uint64 pos in
  let h_header_version,pos = unmarshal_uint32 pos in
  let h_max_table_entries,pos = unmarshal_uint32 pos in
  let h_block_size,pos = unmarshal_uint32 pos in
  let h_checksum,pos = unmarshal_uint32 pos in
  lwt list,pos = unmarshal_n 16 pos (fun pos -> Lwt.return (unmarshal_uint8 pos)) in
  let parent_uuid = uuidarr_to_string (Array.of_list list) in
  let h_parent_time_stamp,pos = unmarshal_uint32 pos in
  let _,pos = unmarshal_uint32 pos in
  let h_parent_unicode_name,pos = unmarshal_utf16_string 512 pos in
  lwt h_parent_locators,pos = unmarshal_parent_locators mmap pos in
  Lwt.return {h_cookie=h_cookie; h_data_offset=h_data_offset;h_table_offset=h_table_offset;
   h_header_version=h_header_version;h_max_table_entries=h_max_table_entries;
   h_block_size=h_block_size; h_checksum=h_checksum; h_parent_unique_id=parent_uuid;
   h_parent_time_stamp=h_parent_time_stamp; h_parent_unicode_name=h_parent_unicode_name;
   h_parent_locators=h_parent_locators}

let read_bat mmap footer header =
  let bat_start = header.h_table_offset in
  let bat_size = Int32.to_int header.h_max_table_entries in
  lwt str = really_read mmap bat_start (Int64.of_int (bat_size * 4)) in
  let pos = (str,0) in
  lwt list,pos = unmarshal_n bat_size pos (fun pos -> Lwt.return (unmarshal_uint32 pos)) in
  Lwt.return (Array.of_list list)

let read_bitmap vhd block =
  let offset = Int64.mul 512L (Int64.of_int32 vhd.bat.(block)) in
  let (block_size,bitmap_size,total_size) = get_block_sizes vhd in
  lwt str = really_read vhd.mmap offset (Int64.of_int32 bitmap_size) in
  let pos = (str,0) in
  lwt list,pos = unmarshal_n (Int32.to_int bitmap_size) pos (fun pos -> Lwt.return (unmarshal_uint8 pos)) in
  Lwt.return (Array.of_list list)

(* Get the filename of the parent VHD if necessary *)
let get_parent_filename header =
  let rec test n =
    if n>=Array.length header.h_parent_locators then (failwith "Failed to find parent!");
    let l = header.h_parent_locators.(n) in
    let open Parent_locator in
    Printf.printf "locator %d\nplatform_code: %s\nplatform_data: %s\n" n (Platform_code.to_string l.platform_code) l.platform_data;
    match l.platform_code with
      | Platform_code.MacX ->
	  begin
	    try 
	      let fname = try String.sub l.platform_data 0 (String.index l.platform_data '\000') with _ -> l.platform_data in
	      let filehdr = String.length "file://" in
	      let fname = String.sub fname filehdr (String.length fname - filehdr) in
	      Printf.printf "Looking for parent: %s\n" fname;
	      ignore(Unix.stat fname); (* Throws an exception if the file doesn't exist *)
	      fname
	    with _ -> test (n+1)
	  end
      | _ -> test (n+1)
  in
  test 0

let rec load_vhd filename =
  lwt fd = Lwt_unix.openfile filename [Unix.O_RDWR] 0o664 in
  let mmap = Lwt_bytes.map_file ~fd:(Lwt_unix.unix_file_descr fd) ~shared:true () in
  lwt footer = read_footer mmap 0L true in
  lwt header = read_header mmap 512L in
  lwt bat = read_bat mmap footer header in
  lwt parent = 
    if footer.Footer.disk_type = Disk_type.Differencing_hard_disk then
      let parent_filename = get_parent_filename header in
      lwt parent = load_vhd parent_filename in
      Lwt.return (Some parent)
    else
      Lwt.return None
  in
  Lwt.return {filename; mmap; header; footer; parent; bat}

(******************************************************************************)
(* Specific VHD marshalling functions                                         *)
(******************************************************************************)

let marshal_features xs = marshal_int32 (Int32.of_int (Feature.to_int xs))

let marshal_geometry geom =
  let open Geometry in
  let output =
    [ marshal_int16 geom.cylinders;
      marshal_int8 geom.heads;
      marshal_int8 geom.sectors ] in
  String.concat "" output

let marshal_disk_type ty = marshal_int32 (Disk_type.to_int32 ty)

let string_to_uuidarr str =
  let list = 
    Scanf.sscanf str "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x" 
      (fun a b c d e f g h i j k l m n o p ->
	[a;b;c;d;e;f;g;h;i;j;k;l;m;n;o;p]) 
  in
  Array.of_list list

let marshal_uuid uuid =
  let arr = 
    try 
      string_to_uuidarr uuid 
    with _ -> [|0;0;0;0;0;0;0;0;0;0;0;0;0;0;0;0|] 
  in
  String.concat "" (List.map marshal_int8 (Array.to_list arr))

let marshal_parent_locator_entry e =
  let open Parent_locator in
  let output =
    [ marshal_int32 (Platform_code.to_int32 e.platform_code);
      marshal_int32 e.platform_data_space_original;
      marshal_int32 e.platform_data_length;
      String.make 4 '\000';
      marshal_int64 e.platform_data_offset ]
  in 
  String.concat "" output

let generic_calc_checksum m =
  let rec inner n cur =
    if n=String.length m then cur else
      inner (n+1) (Int32.add cur (Int32.of_int (int_of_char m.[n])))
  in 
  Int32.lognot (inner 0 0l)

let marshal_footer_no_checksum f =
  let open Footer in
  let output =
    [ f.cookie;
      marshal_features f.features;
      marshal_int32 f.format_version;
      marshal_int64 f.data_offset;
      marshal_int32 f.time_stamp;
      f.creator_application;
      marshal_int32 f.creator_version;
      Host_OS.to_bytes f.creator_host_os;
      marshal_int64 f.original_size;
      marshal_int64 f.current_size;
      marshal_geometry f.geometry;
      marshal_disk_type f.disk_type;
      (String.make 4 '\000'); (* checksum - calculate afterwards *)
      marshal_uuid f.uid;
      marshal_int8 (if f.saved_state then 1 else 0) ] 
  in
  pad_string_to (String.concat "" output) 512

let calc_checksum_footer f =
  let marshalled = marshal_footer_no_checksum f in
  let checksum = generic_calc_checksum marshalled in
  checksum,marshalled

let marshal_footer f =
  let checksum,marshalled = calc_checksum_footer f in
  let checksum_m = marshal_int32 checksum in
  String.blit checksum_m 0 marshalled 64 4;
  marshalled

let marshal_header_no_checksum h =
  let output = 
    [ h.h_cookie;
      marshal_int64 h.h_data_offset;
      marshal_int64 h.h_table_offset;
      marshal_int32 h.h_header_version;
      marshal_int32 h.h_max_table_entries;
      marshal_int32 h.h_block_size;
      String.make 4 '\000';
      marshal_uuid h.h_parent_unique_id;
      marshal_int32 h.h_parent_time_stamp;
      String.make 4 '\000';
      pad_string_to (marshal_utf16 h.h_parent_unicode_name) 512;
      marshal_parent_locator_entry h.h_parent_locators.(0);
      marshal_parent_locator_entry h.h_parent_locators.(1);
      marshal_parent_locator_entry h.h_parent_locators.(2);
      marshal_parent_locator_entry h.h_parent_locators.(3);
      marshal_parent_locator_entry h.h_parent_locators.(4);
      marshal_parent_locator_entry h.h_parent_locators.(5);
      marshal_parent_locator_entry h.h_parent_locators.(6);
      marshal_parent_locator_entry h.h_parent_locators.(7);
    ] 
  in
  pad_string_to (String.concat "" output) 1024

let calc_checksum_header h =
  let marshalled = marshal_header_no_checksum h in
  let checksum = generic_calc_checksum marshalled in
  checksum,marshalled

let marshal_header h =
  let checksum,marshalled = calc_checksum_header h in
  let checksum_m = marshal_int32 checksum in
  String.blit checksum_m 0 marshalled 36 4;
  marshalled

(* Now we do some actual seeking within the file - because this stuff involves *)
(* absolute offsets *)

(* Only write those that actually have a platform_code *)
let write_locator mmap entry =
  let open Parent_locator in
  if entry.platform_code <> Platform_code.None then begin
    really_write mmap entry.platform_data_offset entry.platform_data
  end else Lwt.return ()

let write_locators mmap header =
  Lwt_list.iter_p (fun entry -> write_locator mmap entry) (Array.to_list header.h_parent_locators)

let write_bat mmap vhd =
  let bat_start = vhd.header.h_table_offset in
  let rec inner i =
	  if i=Array.length vhd.bat then Lwt.return () else begin
		  let entry = vhd.bat.(i) in
		  lwt () = really_write mmap (Int64.add bat_start (Int64.mul (Int64.of_int i) 4L)) (marshal_int32 entry) in
		  inner (i+1)
      end
  in inner 0														
      
(******************************************************************************)
(* Debug dumping stuff - dump to screen                                       *)
(******************************************************************************)

let dump_sector sector =
  if String.length sector=0 then
    Printf.printf "Empty sector\n"
  else
    for i=0 to String.length sector - 1 do
      if (i mod 16 = 15) then
	Printf.printf "%02x\n" (Char.code sector.[i])
      else
	Printf.printf "%02x " (Char.code sector.[i])
    done

let dump_footer f =
  let open Footer in
  Printf.printf "VHD FOOTER\n";
  Printf.printf "-=-=-=-=-=\n\n";
  Printf.printf "cookie              : %s\n" f.cookie;
  Printf.printf "features            : %s\n" (String.concat "," (List.map Feature.to_string f.features));
  Printf.printf "format_version      : 0x%lx\n" f.format_version;
  Printf.printf "data_offset         : 0x%Lx\n" f.data_offset;
  Printf.printf "time_stamp          : %lu\n" f.time_stamp;
  Printf.printf "creator_application : %s\n" f.creator_application;
  Printf.printf "creator_version     : 0x%lx\n" f.creator_version;
  Printf.printf "creator_host_os     : %s\n" (Host_OS.to_string f.creator_host_os);
  Printf.printf "original_size       : 0x%Lx\n" f.original_size;
  Printf.printf "current_size        : 0x%Lx\n" f.current_size;
  Printf.printf "geometry            : %s\n" (Geometry.to_string f.geometry);
  Printf.printf "disk_type           : %s\n" (Disk_type.to_string f.disk_type);
  Printf.printf "checksum            : %lu\n" f.checksum;
  Printf.printf "uid                 : %s\n" f.uid;
  Printf.printf "saved_state         : %b\n\n" f.saved_state

let dump_header f =
  Printf.printf "VHD HEADER\n";
  Printf.printf "-=-=-=-=-=\n";
  Printf.printf "cookie              : %s\n" f.h_cookie;
  Printf.printf "data_offset         : %Lx\n" f.h_data_offset;
  Printf.printf "table_offset        : %Lu\n" f.h_table_offset;
  Printf.printf "header_version      : 0x%lx\n" f.h_header_version;
  Printf.printf "max_table_entries   : 0x%lx\n" f.h_max_table_entries;
  Printf.printf "block_size          : 0x%lx\n" f.h_block_size;
  Printf.printf "checksum            : %lu\n" f.h_checksum;
  Printf.printf "parent_unique_id    : %s\n" f.h_parent_unique_id;
  Printf.printf "parent_time_stamp   : %lu\n" f.h_parent_time_stamp;
  Printf.printf "parent_unicode_name : '%s' (%d bytes)\n" (utf16_to_string f.h_parent_unicode_name) (Array.length f.h_parent_unicode_name);
  Printf.printf "parent_locators     : %s\n" 
    (String.concat "\n                      " (List.map Parent_locator.to_string (Array.to_list f.h_parent_locators)))
    
let dump_bat b =
  Printf.printf "BAT\n";
  Printf.printf "-=-\n";
  Array.iteri (fun i x -> Printf.printf "%d\t:0x%lx\n" i x) b

let rec dump_vhd vhd =
  Printf.printf "VHD file: %s\n" vhd.filename;
  dump_header vhd.header;
  dump_footer vhd.footer;
  match vhd.parent with
      None -> ()
    | Some vhd2 -> dump_vhd vhd2

(******************************************************************************)
(* Sector access                                                              *)
(******************************************************************************)

let get_offset_info_of_sector vhd sector =
  let block_size_in_sectors = Int64.div (Int64.of_int32 vhd.header.h_block_size) 512L in
  let block_num = Int64.to_int (Int64.div sector block_size_in_sectors) in
  let sec_in_block = Int64.rem sector block_size_in_sectors in
  let bitmap_byte = Int64.div sec_in_block 8L in
  let bitmap_bit = Int64.rem sec_in_block 8L in
  let mask = 0x80 lsr (Int64.to_int bitmap_bit) in
  let bitmap_size = Int64.div block_size_in_sectors 8L in
  let blockpos = Int64.mul 512L (Int64.of_int32 vhd.bat.(block_num)) in
  let datapos = Int64.add bitmap_size blockpos in
  let sectorpos = Int64.add datapos (Int64.mul sec_in_block 512L) in
  let bitmap_byte_pos = Int64.add bitmap_byte blockpos in
  (block_num,sec_in_block,Int64.to_int bitmap_size, Int64.to_int bitmap_byte,Int64.to_int bitmap_bit,mask,sectorpos,bitmap_byte_pos)

let rec get_sector_pos vhd sector =
  if Int64.mul sector 512L > vhd.footer.Footer.current_size then
    failwith "Sector out of bounds";

  let (block_num,sec_in_block,bitmap_size,bitmap_byte,bitmap_bit,mask,sectorpos,bitmap_byte_pos) = 
    get_offset_info_of_sector vhd sector in

  let maybe_get_from_parent () =
    match vhd.footer.Footer.disk_type,vhd.parent with
      | Disk_type.Differencing_hard_disk,Some vhd2 -> get_sector_pos vhd2 sector
      | Disk_type.Differencing_hard_disk,None -> failwith "Sector in parent but no parent found!"
      | Disk_type.Dynamic_hard_disk,_ -> Lwt.return None
  in

  if (vhd.bat.(block_num) = 0xffffffffl)
  then
    maybe_get_from_parent ()
  else 
    begin      
      lwt bitmap = read_bitmap vhd block_num in
      if vhd.footer.Footer.disk_type = Disk_type.Differencing_hard_disk &&
	bitmap.(bitmap_byte) land mask <> mask
      then
	maybe_get_from_parent ()
      else
	begin
	  lwt str = really_read vhd.mmap sectorpos 512L in
	  Lwt.return (Some (vhd.mmap, sectorpos))
	end
    end    

exception EmptyVHD
let get_top_unused_offset vhd =
  try
    let max_entry_offset = 
      let entries = List.filter (fun x -> x<>unusedl) (Array.to_list vhd.bat) in
      if List.length entries = 0 then raise EmptyVHD;
      let max_entry = List.hd (List.rev (List.sort Int32.compare entries)) in
      max_entry
    in
    let byte_offset = Int64.mul 512L (Int64.of_int32 max_entry_offset) in
    let (block_size,bitmap_size,total_size) = get_block_sizes vhd in
    let total_offset = Int64.add byte_offset (Int64.of_int32 total_size) in
    total_offset
  with 
    | EmptyVHD ->
	let pos = Int64.add vhd.header.h_table_offset 
	  (Int64.mul 4L (Int64.of_int32 vhd.header.h_max_table_entries)) in
	pos
     
let write_trailing_footer vhd =
  let pos = get_top_unused_offset vhd in
  really_write vhd.mmap pos (marshal_footer vhd.footer)
    
let write_vhd vhd =
  lwt () = really_write vhd.mmap 0L (marshal_footer vhd.footer) in
  lwt () = really_write vhd.mmap (vhd.footer.Footer.data_offset) (marshal_header vhd.header) in
  lwt () = write_locators vhd.mmap vhd.header in
  lwt () = write_bat vhd.mmap vhd in
  (* Assume the data is there, or will be written later *)
  lwt () = write_trailing_footer vhd in
  Lwt.return ()
    
let write_clean_block vhd block_num =
  let (block_size, bitmap_size, total) = get_block_sizes vhd in
  let offset = Int64.mul (Int64.of_int32 vhd.bat.(block_num)) 512L in
  let bitmap=String.make (Int32.to_int bitmap_size) '\000' in
  ignore(really_write vhd.mmap offset bitmap);
  let block_size_in_sectors = Int32.to_int (Int32.div vhd.header.h_block_size 512l) in
  let sector=String.make sector_size '\000' in
  for_lwt i=0 to block_size_in_sectors do
	  let pos = Int64.add offset (Int64.of_int (sector_size * i)) in
      really_write vhd.mmap pos sector
  done

let write_sector vhd sector data =
  let block_size_in_sectors = Int64.div (Int64.of_int32 vhd.header.h_block_size) 512L in
  let block_num = Int64.to_int (Int64.div sector block_size_in_sectors) in
  lwt () = 
  if (vhd.bat.(block_num) = unusedl)
  then
    begin
      (* Allocate a new sector *)
      let pos = get_top_unused_offset vhd in (* in bytes *)
      let sec = Int64.div (Int64.add pos 511L) 512L in 
      vhd.bat.(block_num) <- Int64.to_int32 sec;
      lwt () = write_clean_block vhd block_num in
      lwt () = write_bat vhd.mmap vhd in
      lwt () = write_trailing_footer vhd in
      Lwt.return ()
    end
  else 
	Lwt.return () in
  let (block_num,sec_in_block,bitmap_size,bitmap_byte,bitmap_bit,mask,sectorpos,bitmap_byte_pos) = 
    get_offset_info_of_sector vhd sector in
  lwt bitmap = read_bitmap vhd block_num in
  bitmap.(bitmap_byte) <- bitmap.(bitmap_byte) lor mask;
  lwt () = really_write vhd.mmap sectorpos data in
  lwt () = really_write vhd.mmap bitmap_byte_pos (String.make 1 (char_of_int bitmap.(bitmap_byte))) in
  Lwt.return ()

let rewrite_block vhd block_number new_block_number =
  ()

(******************************************************************************)
(* Sanity checks                                                              *)
(******************************************************************************)

type block_marker = 
    | Start of (string * int64)
    | End of (string * int64)

(* Nb this only copes with dynamic or differencing disks *)
let check_overlapping_blocks vhd = 
  let tomarkers name start length =
    [Start (name,start); End (name,Int64.sub (Int64.add start length) 1L)] 
  in
  let blocks = tomarkers "footer_at_top" 0L 512L in
  let blocks = (tomarkers "header" vhd.footer.Footer.data_offset 1024L) @ blocks in
  let blocks =
    if vhd.footer.Footer.disk_type = Disk_type.Differencing_hard_disk 
    then
      begin
	let locators = Array.mapi (fun i l -> (i,l)) vhd.header.h_parent_locators in
	let locators = Array.to_list locators in
        let open Parent_locator in
	let locators = List.filter (fun (_,l) -> l.platform_code <> Platform_code.None) locators in
	let locations = List.map (fun (i,l) -> 
	  let name = Printf.sprintf "locator block %d" i in
	  let start = l.platform_data_offset in
	  let length = Int64.of_int32 l.platform_data_space in
	  tomarkers name start length) locators in
	(List.flatten locations) @ blocks
      end
    else
      blocks
  in
  let bat_start = vhd.header.h_table_offset in
  let bat_size = Int64.of_int32 vhd.header.h_max_table_entries in
  let bat = tomarkers "BAT" bat_start bat_size in
  let blocks = bat @ blocks in
  let bat_blocks = Array.to_list (Array.mapi (fun i b -> (i,b)) vhd.bat) in
  let bat_blocks = List.filter (fun (_,b) -> b <> unusedl) bat_blocks in
  let bat_blocks = List.map (fun (i,b) ->
    let name = Printf.sprintf "block %d" i in
    let start = Int64.mul 512L (Int64.of_int32 vhd.bat.(i)) in
    let size = Int64.of_int32 vhd.header.h_block_size in
    tomarkers name start size) bat_blocks in
  let blocks = (List.flatten bat_blocks) @ blocks in
  let get_pos = function | Start (_,a) -> a | End (_,a) -> a in
  let to_string = function
    | Start (name,pos) -> Printf.sprintf "%Lx START of section '%s'" pos name
    | End (name,pos) -> Printf.sprintf "%Lx END of section '%s'" pos name
  in
  let l = List.sort (fun a b -> compare (get_pos a) (get_pos b)) blocks in
  List.iter (fun marker -> Printf.printf "%s\n" (to_string marker)) l

(* Constructors *)

(* Create a completely new sparse VHD file *)
let create_new_dynamic filename requested_size uuid ?(sparse=true) ?(table_offset=2048L) 
    ?(block_size=block_size) ?(data_offset=512L) ?(saved_state=false)
    ?(features=[Feature.Temporary]) () =

  (* Round up to the nearest 2-meg block *)
  let size = Int64.mul (Int64.div (Int64.add 2097151L requested_size) 2097152L) 2097152L in

  let geometry = 
    let (cyls,heads,secs) = get_chs (Int64.to_int (Int64.div size sector_sizeL)) in
    {
      Geometry.cylinders = cyls; 
      heads; 
      sectors=secs
    }
  in
  let footer = 
    {
      Footer.cookie = footer_cookie;
      features;
      format_version = footer_version;
      data_offset;
      time_stamp = 0l;
      creator_application; creator_version;
      creator_host_os = Host_OS.Other "\000\000\000\000";
      original_size = size;
      current_size = size;
      geometry;
      disk_type = Disk_type.Dynamic_hard_disk;
      checksum = 0l; (* Filled in later *)
      uid = uuid;
      saved_state;
    }
  in
  let header = 
    {
      h_cookie = header_cookie;
      h_data_offset = unusedL;
      h_table_offset = table_offset; (* Stick the BAT at this offset *)
      h_header_version = header_version;
      h_max_table_entries = Int64.to_int32 (Int64.div size (Int64.of_int32 block_size));
      h_block_size = block_size;
      h_checksum = 0l;
      h_parent_unique_id = "";
      h_parent_time_stamp = 0l;
      h_parent_unicode_name = [| |];
      h_parent_locators = Array.make 8 Parent_locator.null
    } 
  in
  let bat = Array.make (Int32.to_int header.h_max_table_entries) unusedl in
  Printf.printf "max_table_entries: %ld (size=%Ld)\n" (header.h_max_table_entries) size;
  lwt fd = Lwt_unix.openfile filename [Unix.O_RDWR; Unix.O_CREAT; Unix.O_EXCL] 0o640 in
  let mmap = Lwt_bytes.map_file ~fd:(Lwt_unix.unix_file_descr fd) ~shared:true ~size:(1024*1024*64) () in
  Lwt.return {filename=filename;
   mmap=mmap;
   header=header;
   footer=footer;
   parent=None;
   bat=bat}

let create_new_difference filename backing_vhd uuid ?(features=[])
    ?(data_offset=512L) ?(saved_state=false) ?(table_offset=2048L) () =
  lwt parent = load_vhd backing_vhd in
  let footer = 
    {
      Footer.cookie = footer_cookie;
      features;
      format_version = footer_version;
      data_offset;
      time_stamp = get_now ();
      creator_application; creator_version;
      creator_host_os = Host_OS.Other "\000\000\000\000";
      original_size = parent.footer.Footer.current_size;
      current_size = parent.footer.Footer.current_size;
      geometry = parent.footer.Footer.geometry;
      disk_type = Disk_type.Differencing_hard_disk;
      checksum = 0l;
      uid = uuid;
      saved_state = saved_state;
    }
  in
  let locator0 = 
    let uri = "file://./" ^ (Filename.basename backing_vhd) in
    {
      Parent_locator.platform_code = Platform_code.MacX;
      platform_data_space = 1l;
      platform_data_space_original=1l;
      platform_data_length = Int32.of_int (String.length uri);
      platform_data_offset = 1536L;
      platform_data = uri;
    }
  in
  let header = 
    {
      h_cookie = header_cookie;
      h_data_offset = unusedL;
      h_table_offset = table_offset;
      h_header_version = header_version;
      h_max_table_entries = parent.header.h_max_table_entries;
      h_block_size = parent.header.h_block_size;
      h_checksum = 0l;
      h_parent_unique_id = parent.footer.Footer.uid;
      h_parent_time_stamp = get_parent_modification_time backing_vhd;
      h_parent_unicode_name = utf16_of_utf8 backing_vhd;
      h_parent_locators = [| locator0; Parent_locator.null; Parent_locator.null; Parent_locator.null;
			     Parent_locator.null; Parent_locator.null; Parent_locator.null; Parent_locator.null; |];
    }
  in
  let bat = Array.make (Int32.to_int header.h_max_table_entries) unusedl in
  lwt fd = Lwt_unix.openfile filename [Unix.O_RDWR; Unix.O_CREAT; Unix.O_EXCL] 0o640 in
  let mmap = Lwt_bytes.map_file ~fd:(Lwt_unix.unix_file_descr fd) ~shared:true () ~size:(1024*1024*64) in
  Lwt.return {filename=filename;
   mmap=mmap;
   header=header;
   footer=footer;
   parent=Some parent;
   bat=bat}

let make_uuid () =
  let arr = Array.init 16 (fun i -> Random.int 255) in
  uuidarr_to_string arr
    
let round_up_to_2mb_block size = 
  let newsize = Int64.mul 2097152L 
    (Int64.div (Int64.add 2097151L size) 2097152L) in
  newsize 
