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

  let of_int32 x =
    if Int32.logand x 1l <> 0l then [ Temporary ] else []

  let to_int32 ts =
    let one = function
      | Temporary -> 1 in
    let reserved = 2 in (* always set *)
    Int32.of_int (List.fold_left (lor) reserved (List.map one ts))

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
    | Other of int32

  let of_int32 = function
    | 0x5769326bl -> Windows
    | 0x4d616320l -> Macintosh
    | x -> Other x

  let to_int32 = function
    | Windows -> 0x5769326bl
    | Macintosh -> 0x4d616320l
    | Other x -> x

  let to_string = function
    | Windows -> "Windows"
    | Macintosh -> "Macintosh"
    | Other x -> Printf.sprintf "Other %lx" x
end

module Geometry = struct
  type t = {
    cylinders : int;
    heads : int;
    sectors : int;
  }

  (* from the Appendix 'CHS calculation' *)
  let of_sectors sectors =
    let max_secs = 65535*255*16 in
    let secs = min max_secs sectors in

    let secs_per_track = ref 0 in
    let heads = ref 0 in
    let cyls_times_heads = ref 0 in
  
    if secs > 65535*63*16 then begin
      secs_per_track := 255;
      heads := 16;
      cyls_times_heads := secs / !secs_per_track;
    end else begin
      secs_per_track := 17;
      cyls_times_heads := secs / !secs_per_track;

      heads := max ((!cyls_times_heads+1023)/1024) 4;

      if (!cyls_times_heads >= (!heads * 1024) || !heads > 16) then begin
        secs_per_track := 31;
        heads := 16;
        cyls_times_heads := secs / !secs_per_track;
      end;

      if (!cyls_times_heads >= (!heads*1024)) then begin
        secs_per_track := 63;
        heads := 16;
        cyls_times_heads := secs / !secs_per_track;
      end	    
    end;
    { cylinders = !cyls_times_heads / !heads; heads = !heads; sectors = !secs_per_track }

  let to_string t = Printf.sprintf "{ cylinders = %d; heads = %d; sectors = %d }"
    t.cylinders t.heads t.sectors

end

(* TODO: use the optimised mirage version *)
let ones_complement_checksum m =
  let rec inner n cur =
    if n=Cstruct.len m then cur else
      inner (n+1) (Int32.add cur (Int32.of_int (Cstruct.get_uint8 m n)))
  in 
  Int32.lognot (inner 0 0l)

module UTF16 = struct
  type t = int array

  let to_utf8 s =
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
        failwith "Bad unicode character!" in
    String.concat "" (List.map (fun c -> Printf.sprintf "%c" c) (List.flatten (List.map utf8_chars_of_int (Array.to_list s))))

  let marshal (buf: Cstruct.t) t =
    let rec inner ofs n =
      if n = Array.length t
      then Cstruct.sub buf 0 ofs
      else begin
        let char = t.(n) in
        if char < 0x10000 then begin
          Cstruct.BE.set_uint16 buf ofs char;
          inner (ofs + 2) (n + 1)
        end else begin
          let char = char - 0x10000 in
          let c1 = (char lsr 10) land 0x3ff in (* high bits *)
          let c2 = char land 0x3ff in (* low bits *)
          Cstruct.BE.set_uint16 buf (ofs + 0) (0xd800 + c1);
          Cstruct.BE.set_uint16 buf (ofs + 2) (0xdc00 + c2);
          inner (ofs + 4) (n + 1)
        end
      end in
    inner 0 0

  let unmarshal (buf: Cstruct.t) len =
    (* Check if there's a byte order marker *)
    let bigendian, pos, max = match Cstruct.BE.get_uint16 buf 0 with
      | 0xfeff -> true,  2, (len / 2 - 1)
      | 0xfffe -> false, 2, (len / 2 - 1)
      | _      -> true,  0, (len / 2) in

    let string = Array.create max 0 in

    let rec inner ofs n =
      if n = max then string
      else begin
        let c = Cstruct.BE.get_uint16 buf ofs in
        let code, ofs, n =
          if c >= 0xd800 && c <= 0xdbff then begin
            let c2 = Cstruct.BE.get_uint16 buf (ofs + 1) in
            if c2 < 0xdc00 || c2 > 0xdfff then (failwith "Bad unicode value!");
            let top10bits = c-0xd800 in
            let bottom10bits = c2-0xdc00 in
            let char = 0x10000 + (bottom10bits lor (top10bits lsl 10)) in
            char, ofs + 2, n + 1
          end else c, ofs + 1, n + 1 in
        string.(n) <- code;
        inner ofs n
      end in
    inner 0 0
end

module Footer = struct
  type t = {
    (* "conectix" *)
    features : Feature.t list;
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
    uid : Uuidm.t;
    saved_state : bool
  }

  let magic = "conectix"

  let expected_version = 0x00010000l

  let dump t =
    Printf.printf "VHD FOOTER\n";
    Printf.printf "-=-=-=-=-=\n\n";
    Printf.printf "cookie              : %s\n" magic;
    Printf.printf "features            : %s\n" (String.concat "," (List.map Feature.to_string t.features));
    Printf.printf "format_version      : 0x%lx\n" expected_version;
    Printf.printf "data_offset         : 0x%Lx\n" t.data_offset;
    Printf.printf "time_stamp          : %lu\n" t.time_stamp;
    Printf.printf "creator_application : %s\n" t.creator_application;
    Printf.printf "creator_version     : 0x%lx\n" t.creator_version;
    Printf.printf "creator_host_os     : %s\n" (Host_OS.to_string t.creator_host_os);
    Printf.printf "original_size       : 0x%Lx\n" t.original_size;
    Printf.printf "current_size        : 0x%Lx\n" t.current_size;
    Printf.printf "geometry            : %s\n" (Geometry.to_string t.geometry);
    Printf.printf "disk_type           : %s\n" (Disk_type.to_string t.disk_type);
    Printf.printf "checksum            : %lu\n" t.checksum;
    Printf.printf "uid                 : %s\n" (Uuidm.to_string t.uid);
    Printf.printf "saved_state         : %b\n\n" t.saved_state

  cstruct footer {
    uint8_t magic[8];
    uint32_t features;
    uint32_t version;
    uint64_t data_offset;
    uint32_t time_stamp;
    uint8_t creator_application[4];
    uint32_t creator_version;
    uint32_t creator_host_os;
    uint64_t original_size;
    uint64_t current_size;
    uint16_t cylinders;
    uint8_t heads;
    uint8_t sectors;
    uint32_t disk_type;
    uint32_t checksum;
    uint8_t uid[16];
    uint8_t saved_state
    (* 427 zeroed *)
  } as big_endian

  let sizeof = 512

  let marshal (buf: Cstruct.t) t =
    set_footer_magic magic 0 buf;
    set_footer_features buf (Feature.to_int32 t.features);
    set_footer_version buf expected_version;
    set_footer_data_offset buf t.data_offset;
    set_footer_time_stamp buf t.time_stamp;
    set_footer_creator_application t.creator_application 0 buf;
    set_footer_creator_version buf t.creator_version;
    set_footer_creator_host_os buf (Host_OS.to_int32 t.creator_host_os);
    set_footer_original_size buf t.original_size;
    set_footer_current_size buf t.current_size;
    set_footer_cylinders buf t.geometry.Geometry.cylinders;
    set_footer_heads buf t.geometry.Geometry.heads;
    set_footer_sectors buf t.geometry.Geometry.sectors;
    set_footer_disk_type buf (Disk_type.to_int32 t.disk_type);
    set_footer_checksum buf 0l;
    set_footer_uid (Uuidm.to_string t.uid) 0 buf;
    set_footer_saved_state buf (if t.saved_state then 1 else 0);
    let remaining = Cstruct.shift buf sizeof_footer in
    for i = 0 to 426 do
      Cstruct.set_uint8 remaining i 0
    done;
    set_footer_checksum buf (ones_complement_checksum (Cstruct.sub buf 0 sizeof))

  let unmarshal (buf: Cstruct.t) =
    let magic' = copy_footer_magic buf in
    if magic' <> magic
    then failwith (Printf.sprintf "Unsupported footer cookie: expected %s, got %s" magic magic');
    let features = Feature.of_int32 (get_footer_features buf) in
    let format_version = get_footer_version buf in
    if format_version <> expected_version
    then failwith (Printf.sprintf "Unsupported footer version: expected %lx, got %lx" expected_version format_version);
    let data_offset = get_footer_data_offset buf in
    let time_stamp = get_footer_time_stamp buf in
    let creator_application = copy_footer_creator_application buf in
    let creator_version = get_footer_creator_version buf in
    let creator_host_os = Host_OS.of_int32 (get_footer_creator_host_os buf) in
    let original_size = get_footer_original_size buf in
    let current_size = get_footer_current_size buf in
    let cylinders = get_footer_cylinders buf in
    let heads = get_footer_heads buf in
    let sectors = get_footer_sectors buf in
    let geometry = { Geometry.cylinders; heads; sectors } in
    let disk_type = Disk_type.of_int32 (get_footer_disk_type buf) in
    let checksum = get_footer_checksum buf in
    let bytes = copy_footer_uid buf in
    let uid = match Uuidm.of_bytes bytes with
      | None -> failwith (Printf.sprintf "Failed to decode UUID: %s" (String.escaped bytes))
      | Some uid -> uid in
    let saved_state = get_footer_saved_state buf = 1 in
    { features; data_offset; time_stamp; creator_version; creator_application;
      creator_host_os; original_size; current_size; geometry; disk_type; checksum; uid; saved_state }
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

  cstruct header {
    uint32_t platform_code;
    uint32_t platform_data_space;
    uint32_t platform_data_length;
    uint32_t reserved;
    uint64_t platform_data_offset
  } as big_endian

  let sizeof = sizeof_header

  let marshal (buf: Cstruct.t) t =
    set_header_platform_code buf (Platform_code.to_int32 t.platform_code);
    set_header_platform_data_space buf t.platform_data_space;
    set_header_platform_data_length buf t.platform_data_length;
    set_header_reserved buf 0l;
    set_header_platform_data_offset buf t.platform_data_offset

  let unmarshal (buf: Cstruct.t) =
    let platform_code = Platform_code.of_int32 (get_header_platform_code buf) in
    let platform_data_space_original = get_header_platform_data_space buf in
    (* WARNING WARNING - see comment on field at the beginning of this file *)
    let platform_data_space =
      if platform_data_space_original < 511l
      then Int32.mul 512l platform_data_space_original
      else platform_data_space_original in
    let platform_data_length = get_header_platform_data_length buf in
    let platform_data_offset = get_header_platform_data_offset buf in
    { platform_code; platform_data_space_original; platform_data_space;
      platform_data_length; platform_data_offset;
      platform_data = "" }
end

module Header = struct

  type t = {
    (* cxsparse *)
    (* 0xFFFFFFFF *)
    table_offset : int64;
    (* 0x00010000l *)
    max_table_entries : int32;
    block_size : int32;
    checksum : int32;
    parent_unique_id : Uuidm.t;
    parent_time_stamp : int32;
    parent_unicode_name : int array;
    parent_locators : Parent_locator.t array;
  }

  (* 1 bit per each 512 byte sector within the block *)
  let sizeof_bitmap t = Int32.(div (div t.block_size 512l) 8l)

  let magic = "cxsparse"

  let expected_data_offset = 0xFFFFFFFFFFFFFFFFL (* XXX: the spec says 8 bytes containing 0xFFFFFFFF *)

  let expected_version = 0x00010000l

  let dump t =
    Printf.printf "VHD HEADER\n";
    Printf.printf "-=-=-=-=-=\n";
    Printf.printf "cookie              : %s\n" magic;
    Printf.printf "data_offset         : %Lx\n" expected_data_offset;
    Printf.printf "table_offset        : %Lu\n" t.table_offset;
    Printf.printf "header_version      : 0x%lx\n" expected_version;
    Printf.printf "max_table_entries   : 0x%lx\n" t.max_table_entries;
    Printf.printf "block_size          : 0x%lx\n" t.block_size;
    Printf.printf "checksum            : %lu\n" t.checksum;
    Printf.printf "parent_unique_id    : %s\n" (Uuidm.to_string t.parent_unique_id);
    Printf.printf "parent_time_stamp   : %lu\n" t.parent_time_stamp;
    Printf.printf "parent_unicode_name : '%s' (%d bytes)\n" (UTF16.to_utf8 t.parent_unicode_name) (Array.length t.parent_unicode_name);
    Printf.printf "parent_locators     : %s\n" 
      (String.concat "\n                      " (List.map Parent_locator.to_string (Array.to_list t.parent_locators)))

  cstruct header {
    uint8_t magic[8];
    uint64_t data_offset;
    uint64_t table_offset;
    uint32_t header_version;
    uint32_t max_table_entries;
    uint32_t block_size;
    uint32_t checksum;
    uint8_t parent_unique_id[16];
    uint32_t parent_time_stamp;
    uint32_t reserved;
    uint8_t parent_unicode_name[512]
    (* 8 parent locators *)
    (* 256 reserved *)
  } as big_endian

  let sizeof = sizeof_header + (8 * Parent_locator.sizeof) + 256

  let unicode_offset = 8 + 8 + 8 + 4 + 4 + 4 + 4 + 16 + 4 + 4

  let marshal (buf: Cstruct.t) t =
    set_header_magic magic 0 buf;
    set_header_data_offset buf expected_data_offset;
    set_header_table_offset buf t.table_offset;
    set_header_header_version buf expected_version;
    set_header_max_table_entries buf t.max_table_entries;
    set_header_block_size buf t.block_size;
    set_header_checksum buf 0l;
    set_header_parent_unique_id (Uuidm.to_bytes t.parent_unique_id) 0 buf;
    set_header_parent_time_stamp buf t.parent_time_stamp;
    set_header_reserved buf 0l;
    for i = 0 to 511 do
      Cstruct.set_uint8 buf (unicode_offset + i) 0
    done;
    let (_: Cstruct.t) = UTF16.marshal (Cstruct.shift buf unicode_offset) t.parent_unicode_name in
    let parent_locators = Cstruct.shift buf (unicode_offset + 512) in
    for i = 0 to 7 do
      let buf = Cstruct.shift parent_locators (Parent_locator.sizeof * i) in
      let pl = if Array.length t.parent_locators <= i then Parent_locator.null else t.parent_locators.(i) in
      Parent_locator.marshal buf pl
    done;
    let reserved = Cstruct.shift parent_locators (8 * Parent_locator.sizeof) in
    for i = 0 to 255 do
      Cstruct.set_uint8 reserved i 0
    done;
    set_header_checksum buf (ones_complement_checksum (Cstruct.sub buf 0 sizeof))

  let unmarshal (buf: Cstruct.t) =
    let magic' = copy_header_magic buf in
    if magic' <> magic
    then failwith (Printf.sprintf "Expected cookie %s, got %s" magic magic');
    let data_offset = get_header_data_offset buf in
    if data_offset <> expected_data_offset
    then failwith (Printf.sprintf "Expected header data_offset %Lx, got %Lx" expected_data_offset data_offset);
    let table_offset = get_header_table_offset buf in
    let header_version = get_header_header_version buf in
    if header_version <> expected_version
    then failwith (Printf.sprintf "Expected header_version %lx, got %lx" expected_version header_version);
    let max_table_entries = get_header_max_table_entries buf in
    let block_size = get_header_block_size buf in
    let checksum = get_header_checksum buf in
    let bytes = copy_header_parent_unique_id buf in
    let parent_unique_id = match (Uuidm.of_bytes bytes) with
      | None -> failwith (Printf.sprintf "Failed to decode UUID: %s" (String.escaped bytes))
      | Some x -> x in
    let parent_time_stamp = get_header_parent_time_stamp buf in
    let parent_unicode_name = UTF16.unmarshal (Cstruct.sub buf unicode_offset 512) 512 in
    let parent_locators_buf = Cstruct.shift buf (unicode_offset + 512) in
    let parent_locators = Array.create 8 Parent_locator.null in
    for i = 0 to 7 do
      let buf = Cstruct.shift parent_locators_buf (Parent_locator.sizeof * i) in
      parent_locators.(i) <- Parent_locator.unmarshal buf
    done;
    { table_offset; max_table_entries; block_size; checksum; parent_unique_id;
      parent_time_stamp; parent_unicode_name; parent_locators }
end

module BAT = struct
  type t = int32 array

  let unused = 0xffffffffl

  let sizeof (header: Header.t) =
    let size_needed = Int32.to_int header.Header.max_table_entries * 4 in
    (* The BAT is always extended to a sector boundary *)
    (size_needed + 511) / 512 * 512

  let unmarshal (buf: Cstruct.t) (header: Header.t) =
    let t = Array.create (Int32.to_int header.Header.max_table_entries) unused in
    for i = 0 to Int32.to_int header.Header.max_table_entries - 1 do
      t.(i) <- Cstruct.BE.get_uint32 buf (i * 4)
    done;
    t
end

module Bitmap = struct
  type t = Cstruct.t

  let sector t sector_in_block = Cstruct.sub t (sector_in_block / 8) 1

  let get t sector_in_block =
    let bitmap_byte = Cstruct.get_uint8 t (sector_in_block / 8) in
    let bitmap_bit = sector_in_block mod 8 in
    let mask = 0x80 lsr bitmap_bit in
    (bitmap_byte land mask) = mask

  let set t sector_in_block =
    let bitmap_byte = Cstruct.get_uint8 t (sector_in_block / 8) in
    let bitmap_bit = sector_in_block mod 8 in
    let mask = 0x80 lsr bitmap_bit in
    Cstruct.set_uint8 t (sector_in_block / 8) (bitmap_byte lor mask)

  let clear t sector_in_block =
    let bitmap_byte = Cstruct.get_uint8 t (sector_in_block / 8) in
    let bitmap_bit = sector_in_block mod 8 in
    let mask = 0x80 lsr bitmap_bit in
    Cstruct.set_uint8 t (sector_in_block / 8) (bitmap_byte land (lnot mask))
end

type vhd = {
  filename : string;
  mmap : Lwt_bytes.t;
  header : Header.t;
  footer : Footer.t;
  parent : vhd option;
  bat : int32 array;
}



let sector_size = 512
let sector_sizeL = 512L
let block_size = 0x200000l


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

(******************************************************************************)
(* Specific VHD unmarshalling functions                                       *)
(******************************************************************************)

let get_block_sizes vhd =
  let block_size = vhd.header.Header.block_size in
  let bitmap_size = Header.sizeof_bitmap vhd.header in
  (block_size, bitmap_size, Int32.add block_size bitmap_size)

let unmarshal_geometry pos =
  let cyl,pos = unmarshal_uint16 pos in
  let heads,pos = unmarshal_uint8 pos in
  let sectors, pos = unmarshal_uint8 pos in
  {Geometry.cylinders = cyl;
   heads; sectors}, pos

let unmarshal_parent_locator mmap pos =
  let open Parent_locator in
  let header = Cstruct.sub (Cstruct.of_bigarray mmap) pos sizeof in
  let p = unmarshal header in
  lwt platform_data = 
    if p.platform_data_length > 0l then
      (Printf.printf "Platform_data_length: %ld\n" p.platform_data_length;
       really_read mmap p.platform_data_offset (Int64.of_int32 p.platform_data_length))
    else Lwt.return "" in
  Lwt.return ({p with platform_data}, pos + sizeof)

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

let read_footer mmap pos =
  let buf = Cstruct.sub (Cstruct.of_bigarray mmap) pos Header.sizeof in
  let f = Footer.unmarshal buf in
  Lwt.return f

let read_header mmap pos =
  let buf = Cstruct.sub (Cstruct.of_bigarray mmap) pos Header.sizeof in
  let h = Header.unmarshal buf in
  (* XXX: parent locator data has not been read *)
  Lwt.return h

let read_bat mmap footer header =
  let buf = Cstruct.sub (Cstruct.of_bigarray mmap) (Int64.to_int header.Header.table_offset) (BAT.sizeof header) in
  let bat = BAT.unmarshal buf header in
  Lwt.return bat

let read_bitmap vhd block =
  let offset = Int64.mul 512L (Int64.of_int32 vhd.bat.(block)) in
  let (block_size,bitmap_size,total_size) = get_block_sizes vhd in
  let bitmap = Cstruct.sub (Cstruct.of_bigarray vhd.mmap) (Int64.to_int offset) (Int32.to_int bitmap_size) in
  Lwt.return bitmap

(* Get the filename of the parent VHD if necessary *)
let get_parent_filename header =
  let rec test n =
    if n>=Array.length header.Header.parent_locators then (failwith "Failed to find parent!");
    let l = header.Header.parent_locators.(n) in
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
  lwt footer = read_footer mmap 0 in
  lwt header = read_header mmap 512 in
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

let marshal_footer f =
  let sector = Cstruct.of_bigarray (Bigarray.(Array1.create char c_layout Footer.sizeof)) in
  Footer.marshal sector f;
  Cstruct.to_string sector

let marshal_header h =
  let buf = Cstruct.of_bigarray (Bigarray.(Array1.create char c_layout Header.sizeof)) in
  Header.marshal buf h;
  Cstruct.to_string buf

(* Now we do some actual seeking within the file - because this stuff involves *)
(* absolute offsets *)

(* Only write those that actually have a platform_code *)
let write_locator mmap entry =
  let open Parent_locator in
  if entry.platform_code <> Platform_code.None then begin
    really_write mmap entry.platform_data_offset entry.platform_data
  end else Lwt.return ()

let write_locators mmap header =
  Lwt_list.iter_p (fun entry -> write_locator mmap entry) (Array.to_list header.Header.parent_locators)

let write_bat mmap vhd =
  let bat_start = vhd.header.Header.table_offset in
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


let dump_bat b =
  Printf.printf "BAT\n";
  Printf.printf "-=-\n";
  Array.iteri (fun i x -> Printf.printf "%d\t:0x%lx\n" i x) b

let rec dump_vhd vhd =
  Printf.printf "VHD file: %s\n" vhd.filename;
  Header.dump vhd.header;
  Footer.dump vhd.footer;
  match vhd.parent with
      None -> ()
    | Some vhd2 -> dump_vhd vhd2

(******************************************************************************)
(* Sector access                                                              *)
(******************************************************************************)

let get_offset_info_of_sector vhd sector =
  let block_size_in_sectors = Int64.div (Int64.of_int32 vhd.header.Header.block_size) 512L in
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
  (block_num,Int64.to_int sec_in_block,Int64.to_int bitmap_size, Int64.to_int bitmap_byte,Int64.to_int bitmap_bit,mask,sectorpos,bitmap_byte_pos)

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
        (not (Bitmap.get bitmap sec_in_block))
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
      let entries = List.filter (fun x -> x<>BAT.unused) (Array.to_list vhd.bat) in
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
	let pos = Int64.add vhd.header.Header.table_offset 
	  (Int64.mul 4L (Int64.of_int32 vhd.header.Header.max_table_entries)) in
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
  let block_size_in_sectors = Int32.to_int (Int32.div vhd.header.Header.block_size 512l) in
  let sector=String.make sector_size '\000' in
  for_lwt i=0 to block_size_in_sectors do
	  let pos = Int64.add offset (Int64.of_int (sector_size * i)) in
      really_write vhd.mmap pos sector
  done

let write_sector vhd sector data =
  let block_size_in_sectors = Int64.div (Int64.of_int32 vhd.header.Header.block_size) 512L in
  let block_num = Int64.to_int (Int64.div sector block_size_in_sectors) in
  lwt () = 
  if (vhd.bat.(block_num) = BAT.unused)
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
  Bitmap.set bitmap sec_in_block;
  lwt () = really_write vhd.mmap sectorpos data in
  lwt () = really_write vhd.mmap bitmap_byte_pos (Cstruct.to_string (Bitmap.sector bitmap sec_in_block)) in
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
	let locators = Array.mapi (fun i l -> (i,l)) vhd.header.Header.parent_locators in
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
  let bat_start = vhd.header.Header.table_offset in
  let bat_size = Int64.of_int32 vhd.header.Header.max_table_entries in
  let bat = tomarkers "BAT" bat_start bat_size in
  let blocks = bat @ blocks in
  let bat_blocks = Array.to_list (Array.mapi (fun i b -> (i,b)) vhd.bat) in
  let bat_blocks = List.filter (fun (_,b) -> b <> BAT.unused) bat_blocks in
  let bat_blocks = List.map (fun (i,b) ->
    let name = Printf.sprintf "block %d" i in
    let start = Int64.mul 512L (Int64.of_int32 vhd.bat.(i)) in
    let size = Int64.of_int32 vhd.header.Header.block_size in
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

let blank_uuid = match Uuidm.of_bytes (String.make 16 '\000') with
  | Some x -> x
  | None -> assert false (* never happens *)

(* Create a completely new sparse VHD file *)
let create_new_dynamic filename requested_size uuid ?(sparse=true) ?(table_offset=2048L) 
    ?(block_size=block_size) ?(data_offset=512L) ?(saved_state=false)
    ?(features=[Feature.Temporary]) () =

  (* Round up to the nearest 2-meg block *)
  let size = Int64.mul (Int64.div (Int64.add 2097151L requested_size) 2097152L) 2097152L in

  let geometry = Geometry.of_sectors (Int64.to_int (Int64.div size sector_sizeL)) in
  let footer = 
    {
      Footer.features;
      data_offset;
      time_stamp = 0l;
      creator_application; creator_version;
      creator_host_os = Host_OS.Other 0l;
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
      Header.table_offset; (* Stick the BAT at this offset *)
      max_table_entries = Int64.to_int32 (Int64.div size (Int64.of_int32 block_size));
      block_size;
      checksum = 0l;
      parent_unique_id = blank_uuid;
      parent_time_stamp = 0l;
      parent_unicode_name = [| |];
      parent_locators = Array.make 8 Parent_locator.null
    } 
  in
  let bat = Array.make (Int32.to_int header.Header.max_table_entries) (BAT.unused) in
  Printf.printf "max_table_entries: %ld (size=%Ld)\n" (header.Header.max_table_entries) size;
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
      Footer.features;
      data_offset;
      time_stamp = get_now ();
      creator_application; creator_version;
      creator_host_os = Host_OS.Other 0l;
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
      Header.table_offset;
      max_table_entries = parent.header.Header.max_table_entries;
      block_size = parent.header.Header.block_size;
      checksum = 0l;
      parent_unique_id = parent.footer.Footer.uid;
      parent_time_stamp = get_parent_modification_time backing_vhd;
      parent_unicode_name = utf16_of_utf8 backing_vhd;
      parent_locators = [| locator0; Parent_locator.null; Parent_locator.null; Parent_locator.null;
			     Parent_locator.null; Parent_locator.null; Parent_locator.null; Parent_locator.null; |];
    }
  in
  let bat = Array.make (Int32.to_int header.Header.max_table_entries) (BAT.unused) in
  lwt fd = Lwt_unix.openfile filename [Unix.O_RDWR; Unix.O_CREAT; Unix.O_EXCL] 0o640 in
  let mmap = Lwt_bytes.map_file ~fd:(Lwt_unix.unix_file_descr fd) ~shared:true () ~size:(1024*1024*64) in
  Lwt.return {filename=filename;
   mmap=mmap;
   header=header;
   footer=footer;
   parent=Some parent;
   bat=bat}

let round_up_to_2mb_block size = 
  let newsize = Int64.mul 2097152L 
    (Int64.div (Int64.add 2097151L size) 2097152L) in
  newsize 
