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

let sector_size = 512

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

  let default_creator_application = "caml"
  let default_creator_version = 0x00000001l

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
    platform_data : Cstruct.t;
  }


  let null = {
    platform_code=Platform_code.None;
    platform_data_space=0l;
    platform_data_space_original=0l;
    platform_data_length=0l;
    platform_data_offset=0L;
    platform_data=Cstruct.create 0;
  }

  let to_string t =
    Printf.sprintf "(%s %lx %lx, %ld, 0x%Lx, %s)" (Platform_code.to_string t.platform_code)
      t.platform_data_space t.platform_data_space_original
      t.platform_data_length t.platform_data_offset (Cstruct.to_string t.platform_data)

  let to_filename t = match t.platform_code with
    | Platform_code.MacX ->
      (* Interpret as a NULL-terminated string *)
      let rec find_string from =
        if Cstruct.len t.platform_data <= from
        then t.platform_data
        else
          if Cstruct.get_uint8 t.platform_data from = 0
          then Cstruct.sub t.platform_data 0 from
          else find_string (from + 1) in
      let path = Cstruct.to_string (find_string 0) in
      let expected_prefix = "file://" in
      let expected_prefix' = String.length expected_prefix in
      let startswith prefix x =
        let prefix' = String.length prefix and x' = String.length x in
        prefix' <= x' && (String.sub x 0 prefix' = prefix) in
      if startswith expected_prefix path
      then Some (String.sub path expected_prefix' (String.length path - expected_prefix'))
      else None
    | _ -> None

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
      platform_data = Cstruct.create 0 }
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

  let default_block_size = 0x200000l

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

  let get_block_sizes t =
    let bitmap_size = sizeof_bitmap t in
    (t.block_size, bitmap_size, Int32.add t.block_size bitmap_size)
end

module BAT = struct
  type t = int32 array

  let unused = 0xffffffffl

  let sizeof (header: Header.t) =
    let size_needed = Int32.to_int header.Header.max_table_entries * 4 in
    (* The BAT is always extended to a sector boundary *)
    (size_needed + 511) / 512 * 512

  let unmarshal (buf: Cstruct.t) (header: Header.t) =
    let t = Array.create (sizeof header) unused in
    for i = 0 to Int32.to_int header.Header.max_table_entries - 1 do
      t.(i) <- Cstruct.BE.get_uint32 buf (i * 4)
    done;
    t

  let marshal (buf: Cstruct.t) (t: t) =
    for i = 0 to Array.length t - 1 do
      Cstruct.BE.set_uint32 buf i t.(i)
    done

  let dump t =
    Printf.printf "BAT\n";
    Printf.printf "-=-\n";
    Array.iteri (fun i x -> Printf.printf "%d\t:0x%lx\n" i x) t
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

module Sector = struct
  type t = Cstruct.t

  let dump t =
    if Cstruct.len t = 0
    then Printf.printf "Empty sector\n"
    else
      for i=0 to Cstruct.len t - 1 do
        if (i mod 16 = 15) then
          Printf.printf "%02x\n" (Cstruct.get_uint8 t i)
        else
          Printf.printf "%02x " (Cstruct.get_uint8 t i)
      done

end

module Vhd = struct
  type 'a t = {
    filename: string;
    handle: 'a;
    header: Header.t;
    footer: Footer.t;
    parent: 'a t option;
    bat: BAT.t;
  }

  let get_offset_info_of_sector t sector =
    let block_size_in_sectors = Int64.div (Int64.of_int32 t.header.Header.block_size) 512L in
    let block_num = Int64.to_int (Int64.div sector block_size_in_sectors) in
    let sec_in_block = Int64.rem sector block_size_in_sectors in
    let bitmap_byte = Int64.div sec_in_block 8L in
    let bitmap_bit = Int64.rem sec_in_block 8L in
    let mask = 0x80 lsr (Int64.to_int bitmap_bit) in
    let bitmap_size = Int64.div block_size_in_sectors 8L in
    let blockpos = Int64.mul 512L (Int64.of_int32 t.bat.(block_num)) in
    let datapos = Int64.add bitmap_size blockpos in
    let sectorpos = Int64.add datapos (Int64.mul sec_in_block 512L) in
    let bitmap_byte_pos = Int64.add bitmap_byte blockpos in
    (block_num,Int64.to_int sec_in_block,Int64.to_int bitmap_size, Int64.to_int bitmap_byte,Int64.to_int bitmap_bit,mask,sectorpos,bitmap_byte_pos)

  let rec dump t =
    Printf.printf "VHD file: %s\n" t.filename;
    Header.dump t.header;
    Footer.dump t.footer;
    match t.parent with
    | None -> ()
    | Some parent -> dump parent

  type block_marker = 
    | Start of (string * int64)
    | End of (string * int64)

  (* Nb this only copes with dynamic or differencing disks *)
  let check_overlapping_blocks t = 
    let tomarkers name start length =
      [Start (name,start); End (name,Int64.sub (Int64.add start length) 1L)] in
    let blocks = tomarkers "footer_at_top" 0L 512L in
    let blocks = (tomarkers "header" t.footer.Footer.data_offset 1024L) @ blocks in
    let blocks =
      if t.footer.Footer.disk_type = Disk_type.Differencing_hard_disk then begin
        let locators = Array.mapi (fun i l -> (i,l)) t.header.Header.parent_locators in
        let locators = Array.to_list locators in
        let open Parent_locator in
        let locators = List.filter (fun (_,l) -> l.platform_code <> Platform_code.None) locators in
        let locations = List.map (fun (i,l) -> 
          let name = Printf.sprintf "locator block %d" i in
          let start = l.platform_data_offset in
          let length = Int64.of_int32 l.platform_data_space in
          tomarkers name start length) locators in
        (List.flatten locations) @ blocks
      end else blocks in
    let bat_start = t.header.Header.table_offset in
    let bat_size = Int64.of_int32 t.header.Header.max_table_entries in
    let bat = tomarkers "BAT" bat_start bat_size in
    let blocks = bat @ blocks in
    let bat_blocks = Array.to_list (Array.mapi (fun i b -> (i,b)) t.bat) in
    let bat_blocks = List.filter (fun (_,b) -> b <> BAT.unused) bat_blocks in
    let bat_blocks = List.map (fun (i,b) ->
      let name = Printf.sprintf "block %d" i in
      let start = Int64.mul 512L (Int64.of_int32 t.bat.(i)) in
      let size = Int64.of_int32 t.header.Header.block_size in
      tomarkers name start size) bat_blocks in
    let blocks = (List.flatten bat_blocks) @ blocks in
    let get_pos = function | Start (_,a) -> a | End (_,a) -> a in
    let to_string = function
    | Start (name,pos) -> Printf.sprintf "%Lx START of section '%s'" pos name
    | End (name,pos) -> Printf.sprintf "%Lx END of section '%s'" pos name in
    let l = List.sort (fun a b -> compare (get_pos a) (get_pos b)) blocks in
    List.iter (fun marker -> Printf.printf "%s\n" (to_string marker)) l

  exception EmptyVHD

  let get_top_unused_offset header bat =
    try
      let max_entry_offset = 
        let entries = List.filter (fun x -> x<>BAT.unused) (Array.to_list bat) in
        if List.length entries = 0 then raise EmptyVHD;
        let max_entry = List.hd (List.rev (List.sort Int32.compare entries)) in
        max_entry in
      let byte_offset = Int64.mul 512L (Int64.of_int32 max_entry_offset) in
      let (block_size,bitmap_size,total_size) = Header.get_block_sizes header in
      let total_offset = Int64.add byte_offset (Int64.of_int32 total_size) in
      total_offset
    with 
      | EmptyVHD ->
        let pos = Int64.add header.Header.table_offset 
          (Int64.mul 4L (Int64.of_int32 header.Header.max_table_entries)) in
        pos
end

module Make = functor(File: S.IO) -> struct
  open File

  module Footer_IO = struct
    open Footer

    let read fd pos =
      really_read fd pos Footer.sizeof >>= fun buf ->
      return (Footer.unmarshal buf)

    let write fd pos t =
      let sector = Cstruct.of_bigarray (Bigarray.(Array1.create char c_layout Footer.sizeof)) in
      Footer.marshal sector t;
      really_write fd pos sector
  end

  module Parent_locator_IO = struct
    open Parent_locator

    let read fd t =
      really_read fd (Int64.to_int t.platform_data_offset) (Int32.to_int t.platform_data_length) >>= fun platform_data ->
      return { t with platform_data }

    let write fd t =
      (* Only write those that actually have a platform_code *)
      if t.platform_code <> Platform_code.None
      then really_write fd (Int64.to_int t.platform_data_offset) t.platform_data
      else return ()
  end

  module Header_IO = struct
    open Header

    let get_parent_filename t =
      let rec test n =
        if n >= Array.length t.parent_locators then (failwith "Failed to find parent!");
        let l = t.parent_locators.(n) in
        let open Parent_locator in
        Printf.printf "locator %d\nplatform_code: %s\nplatform_data: %s\n" n (Platform_code.to_string l.platform_code) (Cstruct.to_string l.platform_data);
        match to_filename l with
        | Some path ->
          exists path >>= fun x ->
          if not x
          then test (n + 1)
          else return path
        | None -> test (n + 1) in
      test 0

    let read fd pos =
      really_read fd pos sizeof >>= fun buf ->
      let t = unmarshal buf in
      (* Read the parent_locator data *)
      let rec read_parent_locator = function
        | 8 -> return ()
        | n ->
          let p = t.parent_locators.(n) in
          let open Parent_locator in
          Parent_locator_IO.read fd p >>= fun p ->
          t.parent_locators.(n) <- p;
          read_parent_locator (n + 1) in
      read_parent_locator 0 >>= fun () ->
      return t  

    let write fd pos t =
      let buf = Cstruct.of_bigarray (Bigarray.(Array1.create char c_layout Header.sizeof)) in
      marshal buf t;
      (* Write the parent_locator data *)
      let rec write_parent_locator = function
        | 8 -> return ()
        | n ->
          let p = t.parent_locators.(n) in
          let open Parent_locator in
          Parent_locator_IO.write fd p >>= fun () ->
          write_parent_locator (n + 1) in
      write_parent_locator 0
  end

  module BAT_IO = struct
    open BAT

    let read fd (header: Header.t) =
      really_read fd (Int64.to_int header.Header.table_offset) (sizeof header) >>= fun buf ->
      return (unmarshal buf header)

    let write fd (header: Header.t) t =
      let buf = Cstruct.of_bigarray (Bigarray.(Array1.create char c_layout (sizeof header))) in
      marshal buf t;
      really_write fd (Int64.to_int header.Header.table_offset) buf
  end

  module Bitmap_IO = struct
    open Bitmap

    let read fd (header: Header.t) (bat: BAT.t) (block: int) =
      let pos = Int64.(to_int (mul 512L (of_int32 bat.(block)))) in
      really_read fd pos (Int32.to_int (Header.sizeof_bitmap header))
  end

  module Vhd_IO = struct
    open Vhd

    let rec openfile filename =
      File.openfile filename >>= fun fd ->
      Footer_IO.read fd 0 >>= fun footer ->
      Header_IO.read fd 512 >>= fun header ->
      BAT_IO.read fd header >>= fun bat ->
      (match footer.Footer.disk_type with
        | Disk_type.Differencing_hard_disk ->
          Header_IO.get_parent_filename header >>= fun parent_filename ->
          openfile parent_filename >>= fun p ->
          return (Some p)
        | _ ->
          return None) >>= fun parent ->
      return { filename; handle = fd; header; footer; bat; parent }

    let rec get_sector_pos t sector =
      if Int64.mul sector 512L > t.Vhd.footer.Footer.current_size then
        failwith "Sector out of bounds";

      let (block_num,sec_in_block,bitmap_size,bitmap_byte,bitmap_bit,mask,sectorpos,bitmap_byte_pos) = 
        Vhd.get_offset_info_of_sector t sector in

      let maybe_get_from_parent () = match t.Vhd.footer.Footer.disk_type,t.Vhd.parent with
        | Disk_type.Differencing_hard_disk,Some vhd2 -> get_sector_pos vhd2 sector
        | Disk_type.Differencing_hard_disk,None -> failwith "Sector in parent but no parent found!"
        | Disk_type.Dynamic_hard_disk,_ -> return None in

      if t.Vhd.bat.(block_num) = BAT.unused
      then maybe_get_from_parent ()
      else begin
        Bitmap_IO.read t.Vhd.handle t.Vhd.header t.Vhd.bat block_num >>= fun bitmap ->
        if t.Vhd.footer.Footer.disk_type = Disk_type.Differencing_hard_disk && (not (Bitmap.get bitmap sec_in_block))
        then maybe_get_from_parent ()
        else begin
          really_read t.Vhd.handle (Int64.to_int sectorpos) 512 >>= fun data ->
          return (Some data)
        end
      end  

    let write_trailing_footer t =
      let pos = Vhd.get_top_unused_offset t.Vhd.header t.Vhd.bat in
      Footer_IO.write t.Vhd.handle (Int64.to_int pos) t.Vhd.footer
    
    let write t =
      Footer_IO.write t.Vhd.handle 0 t.Vhd.footer >>= fun () ->
      Header_IO.write t.Vhd.handle (Int64.to_int t.Vhd.footer.Footer.data_offset) t.Vhd.header >>= fun () ->
      BAT_IO.write t.Vhd.handle t.Vhd.header t.Vhd.bat >>= fun () ->
      (* Assume the data is there, or will be written later *)
      write_trailing_footer t

    let write_zero_block t block_num =
      let (block_size, bitmap_size, total) = Header.get_block_sizes t.Vhd.header in
      let bitmap_size = Int32.to_int bitmap_size in
      let offset = Int64.mul (Int64.of_int32 t.Vhd.bat.(block_num)) 512L in
      let bitmap = Cstruct.of_bigarray (Bigarray.(Array1.create char c_layout bitmap_size)) in
      for i = 0 to bitmap_size - 1 do
        Cstruct.set_uint8 bitmap i 0
      done;
      really_write t.Vhd.handle (Int64.to_int offset) bitmap >>= fun () ->
      let block_size_in_sectors = Int32.to_int (Int32.div t.Vhd.header.Header.block_size 512l) in
      let sector = Cstruct.of_bigarray (Bigarray.(Array1.create char c_layout 512)) in
      for i = 0 to 511 do
        Cstruct.set_uint8 sector i 0
      done;
      let rec loop n =
        if n >= block_size_in_sectors
        then return ()
        else
          let pos = Int64.(add offset (of_int (sector_size * n))) in
          really_write t.Vhd.handle (Int64.to_int pos) sector >>= fun () ->
          loop (n + 1) in
      loop 0

    let write_sector t sector data =
      let block_size_in_sectors = Int64.div (Int64.of_int32 t.Vhd.header.Header.block_size) 512L in
      let block_num = Int64.to_int (Int64.div sector block_size_in_sectors) in

      let update_sector () =
        let (block_num,sec_in_block,bitmap_size,bitmap_byte,bitmap_bit,mask,sectorpos,bitmap_byte_pos) = 
          Vhd.get_offset_info_of_sector t sector in
        Bitmap_IO.read t.Vhd.handle t.Vhd.header t.Vhd.bat block_num >>= fun bitmap ->
        Bitmap.set bitmap sec_in_block;
        really_write t.Vhd.handle (Int64.to_int sectorpos) data >>= fun () ->
        really_write t.Vhd.handle (Int64.to_int bitmap_byte_pos) (Bitmap.sector bitmap sec_in_block) in

      if t.Vhd.bat.(block_num) = BAT.unused then begin
        (* Allocate a new sector *)
        let pos = Vhd.get_top_unused_offset t.Vhd.header t.Vhd.bat in (* in bytes *)
        let sec = Int64.div (Int64.add pos 511L) 512L in 
        t.Vhd.bat.(block_num) <- Int64.to_int32 sec;
        write_zero_block t block_num >>= fun () ->
        BAT_IO.write t.Vhd.handle t.Vhd.header t.Vhd.bat >>= fun () ->
        write_trailing_footer t >>= fun () ->
        update_sector ()
      end else update_sector ()

  end

end
