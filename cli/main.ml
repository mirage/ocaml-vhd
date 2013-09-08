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

let project_url = "http://github.com/djs55/ocaml-vhd"

open Common
open Cmdliner

(* Help sections common to all commands *)

let _common_options = "COMMON OPTIONS"
let help = [ 
 `S _common_options; 
 `P "These options are common to all commands.";
 `S "MORE HELP";
 `P "Use `$(mname) $(i,COMMAND) --help' for help on a single command."; `Noblank;
 `S "BUGS"; `P (Printf.sprintf "Check bug reports at %s" project_url);
]

(* Options common to all commands *)
let common_options_t = 
  let docs = _common_options in 
  let debug = 
    let doc = "Give only debug output." in
    Arg.(value & flag & info ["debug"] ~docs ~doc) in
  let verb =
    let doc = "Give verbose output." in
    let verbose = true, Arg.info ["v"; "verbose"] ~docs ~doc in 
    Arg.(last & vflag_all [false] [verbose]) in 
  Term.(pure Common.make $ debug $ verb)

let parse_size x =
  let kib = 1024L in
  let mib = Int64.mul kib kib in
  let gib = Int64.mul mib kib in
  let tib = Int64.mul gib kib in
  let endswith suffix x =
    let suffix' = String.length suffix in
    let x' = String.length x in
    x' >= suffix' && (String.sub x (x' - suffix') suffix' = suffix) in
  let remove suffix x =
    let suffix' = String.length suffix in
    let x' = String.length x in
    String.sub x 0 (x' - suffix') in
  try
    if endswith "KiB" x then Int64.(mul kib (of_string (remove "KiB" x)))
    else if endswith "MiB" x then Int64.(mul mib (of_string (remove "MiB" x)))
    else if endswith "GiB" x then Int64.(mul gib (of_string (remove "GiB" x)))
    else if endswith "TiB" x then Int64.(mul tib (of_string (remove "TiB" x)))
    else Int64.of_string x
  with _ ->
    failwith (Printf.sprintf "Cannot parse size: %s" x)

module type Floatable = sig
  type t
  val to_float: t -> float
end

module Progress_bar(T: Floatable) = struct
  type t = {
    max_value: T.t;
    mutable current_value: T.t;
    width: int;
    line: string;
    mutable spin_index: int;
  }

  let prefix_s = "[*] "
  let prefix = String.length prefix_s
  let suffix_s = "  (   %)"
  let suffix = String.length suffix_s

  let spinner = [| '-'; '\\'; '|'; '/' |]

  let create width current_value max_value =
    let line = String.make width ' ' in
    String.blit prefix_s 0 line 0 prefix;
    String.blit suffix_s 0 line (width - suffix - 1) suffix;
    let spin_index = 0 in
    { max_value; current_value; width; line; spin_index }

  let percent t = int_of_float (T.(to_float t.current_value /. (to_float t.max_value) *. 100.))

  let bar_width t value =
    int_of_float (T.(to_float value /. (to_float t.max_value) *. (float_of_int (t.width - prefix - suffix))))

  let print_bar t =
    let w = bar_width t t.current_value in
    t.line.[1] <- spinner.(t.spin_index);
    t.spin_index <- (t.spin_index + 1) mod (Array.length spinner);
    for i = 0 to w - 1 do
      t.line.[prefix + i] <- (if i = w - 1 then '>' else '#')
    done;
    let percent = Printf.sprintf "%3d" (percent t) in
    String.blit percent 0 t.line (t.width - 6) 3;
    Printf.printf "\r%s%!" t.line

  let update t new_value =
    let old_bar = bar_width t t.current_value in
    let new_bar = bar_width t new_value in
    t.current_value <- new_value;
    if new_bar <> old_bar then print_bar t
end

module Impl = struct
  open Lwt
  open Vhd
  open Vhd_lwt

  let require name arg = match arg with
    | None -> failwith (Printf.sprintf "Please supply a %s argument" name)
    | Some x -> x

  type field = {
    name: string;
    get: Vhd_lwt.Vhd_IO.handle Vhd.t -> string Lwt.t;
  }

  let fields = [
    {
      name = "features";
      get = fun t -> return (String.concat ", " (List.map Feature.to_string t.Vhd.footer.Footer.features));
    }; {
      name = "data-offset";
      get = fun t -> return (Int64.to_string t.Vhd.footer.Footer.data_offset);
    }; {
      name = "time-stamp";
      get = fun t -> return (Int32.to_string t.Vhd.footer.Footer.time_stamp);
    }; {
      name = "creator-application";
      get = fun t -> return t.Vhd.footer.Footer.creator_application;
    }; {
      name = "creator-version";
      get = fun t -> return (Int32.to_string t.Vhd.footer.Footer.creator_version);
    }; {
      name = "creator-host-os";
      get = fun t -> return (Host_OS.to_string t.Vhd.footer.Footer.creator_host_os);
    }; {
      name = "original-size";
      get = fun t -> return (Int64.to_string t.Vhd.footer.Footer.original_size);
    }; {
      name = "current-size";
      get = fun t -> return (Int64.to_string t.Vhd.footer.Footer.current_size);
    }; {
      name = "geometry";
      get = fun t -> return (Geometry.to_string t.Vhd.footer.Footer.geometry);
    }; {
      name = "disk-type";
      get = fun t -> return (Disk_type.to_string t.Vhd.footer.Footer.disk_type);
    }; {
      name = "footer-checksum";
      get = fun t -> return (Int32.to_string t.Vhd.footer.Footer.checksum);
    }; {
      name = "uuid";
      get = fun t -> return (Uuidm.to_string t.Vhd.footer.Footer.uid);
    }; {
      name = "saved-state";
      get = fun t -> return (string_of_bool t.Vhd.footer.Footer.saved_state);
    }; {
      name = "table-offset";
      get = fun t -> return (Int64.to_string t.Vhd.header.Header.table_offset);
    }; {
      name = "max-table-entries";
      get = fun t -> return (string_of_int t.Vhd.header.Header.max_table_entries);
    }; {
      name = "block-size-sectors-shift";
      get = fun t -> return (string_of_int t.Vhd.header.Header.block_size_sectors_shift);
    }; {
      name = "header-checksum";
      get = fun t -> return (Int32.to_string t.Vhd.header.Header.checksum);
    }; {
      name = "parent-uuid";
      get = fun t -> return (Uuidm.to_string t.Vhd.header.Header.parent_unique_id);
    }; {
      name = "parent-time-stamp";
      get = fun t -> return (Int32.to_string t.Vhd.header.Header.parent_time_stamp);
    }; {
      name = "parent-unicode-name";
      get = fun t -> return (UTF16.to_utf8_exn t.Vhd.header.Header.parent_unicode_name);
    };
  ] @ (List.map (fun i ->
    {
      name = Printf.sprintf "parent-locator-%d" i;
      get = fun t -> return (Parent_locator.to_string t.Vhd.header.Header.parent_locators.(i));
    }) [ 0; 1; 2; 3; 4; 5; 6; 7 ])

  let get common filename key =
    try
      let filename = require "filename" filename in
      let key = require "key" key in
      let field = List.find (fun f -> f.name = key) fields in
      let t =
        lwt t = Vhd_IO.openfile filename in
        lwt result = field.get t in
        Printf.printf "%s\n" result;
        Vhd_IO.close t in
      Lwt_main.run t;
      `Ok ()
    with
      | Failure x ->
        `Error(true, x)
      | Not_found ->
        `Error(true, Printf.sprintf "Unknown key. Known keys are: %s" (String.concat ", " (List.map (fun f -> f.name) fields)))

  let padto blank n s =
    let result = String.make n blank in
    String.blit s 0 result 0 (min n (String.length s));
    result

  let print_table header rows =
    let nth xs i = try List.nth xs i with Not_found -> "" in
    let width_of_column i =
      let values = nth header i :: (List.map (fun r -> nth r i) rows) in
      let widths = List.map String.length values in
      List.fold_left max 0 widths in
    let widths = List.rev (snd(List.fold_left (fun (i, acc) _ -> (i + 1, (width_of_column i) :: acc)) (0, []) header)) in
    let print_row row =
      List.iter (fun (n, s) -> Printf.printf "%s |" (padto ' ' n s)) (List.combine widths row);
      Printf.printf "\n" in
    print_row header;
    List.iter (fun (n, _) -> Printf.printf "%s-|" (padto '-' n "")) (List.combine widths header);
    Printf.printf "\n";
    List.iter print_row rows
    

  let info common filename =
    try
      let filename = require "filename" filename in
      let t =
        lwt t = Vhd_IO.openfile filename in
        lwt all = Lwt_list.map_s (fun f ->
          lwt v = f.get t in
          return [ f.name; v ]
        ) fields in
        print_table ["field"; "value"] all;
        return () in
      Lwt_main.run t;
      `Ok ()
    with Failure x ->
      `Error(true, x)

  let create common filename size parent =
    try
      begin let filename = require "filename" filename in
      match parent, size with
      | None, None -> failwith "Please supply either a size or a parent"
      | None, Some size ->
        let size = parse_size size in
        let t =
          lwt vhd = Vhd_IO.create_dynamic ~filename ~size () in
          Vhd_IO.close vhd in
        Lwt_main.run t
      | Some parent, None ->
        let t =
          lwt parent = Vhd_IO.openfile parent in
          lwt vhd = Vhd_IO.create_difference ~filename ~parent () in
          lwt () = Vhd_IO.close parent in
          lwt () = Vhd_IO.close vhd in
          return () in
        Lwt_main.run t
      | Some parent, Some size ->
        failwith "Overriding the size in a child node not currently implemented"
      end;
      `Ok ()
    with Failure x ->
      `Error(true, x)

  let check common filename =
    try
      let filename = require "filename" filename in
      let t =
        lwt vhd = Vhd_IO.openfile filename in
        Vhd.check_overlapping_blocks vhd;
        return () in
      Lwt_main.run t;
      `Ok ()
    with Failure x ->
      `Error(true, x)

  let stream_human common t s =
    (* How much space will we need for the sector numbers? *)
    let bytes = t.Vhd.footer.Footer.current_size in
    let sectors = Int64.shift_right bytes sector_shift in
    let decimal_digits = int_of_float (ceil (log10 (Int64.to_float sectors))) in
    Printf.printf "# beginning of stream\n";
    Printf.printf "# offset : contents\n";
    lwt _ = fold_left (fun sector x ->
      Printf.printf "%s: %s\n"
        (padto ' ' decimal_digits (string_of_int sector))
        (Element.to_string x);
      return (sector + (Element.len x))
    ) 0 s in
    Printf.printf "# end of stream\n";
    return ()

  module P = Progress_bar(Int64)

  let stream_nbd common t s prezeroed progress =
    let sock = Lwt_unix.socket Unix.PF_UNIX Unix.SOCK_STREAM 0 in
    let sockaddr = Lwt_unix.ADDR_UNIX "socket" in
    lwt () = Lwt_unix.connect sock sockaddr in

    lwt (server, size, flags) = Nbd_lwt_client.negotiate sock in

    let twomib_bytes = 2 * 1024 * 1024 in
    let twomib_sectors = twomib_bytes / 512 in
    let twomib_empty =
      let b = Cstruct.create twomib_bytes in
      for i = 0 to twomib_bytes - 1 do
        Cstruct.set_uint8 b i 0
      done;
      b in

    (* TODO: we could precompute the actual amount of work in the stream *)
    let bytes = t.Vhd.footer.Footer.current_size in
    let p = P.create 80 0L (Int64.div bytes 512L) in

    lwt _ = fold_left (fun sector x ->
      lwt () = match x with
      | Element.Copy(h, sector_start, sector_len) ->
        let rec copy sector sector_start sector_len =
          let this = min sector_len twomib_sectors in
          lwt data = Fd.really_read h (Int64.mul sector_start 512L) (this * 512) in
          lwt () = Nbd_lwt_client.write server data (Int64.mul sector 512L) in
          let sector_len = sector_len - this in
          let sector_start = Int64.(add sector_start (of_int this)) in
          let sector = Int64.(add sector (of_int this)) in
          if progress then P.update p sector;
          if sector_len > 0 then copy sector sector_start sector_len else return () in
        copy sector sector_start sector_len
      | Element.Sectors data ->
        lwt () = Nbd_lwt_client.write server data (Int64.mul sector 512L) in
        if progress then P.update p sector;
        return ()
      | Element.Empty n ->
        if not prezeroed then begin
          let rec copy sector n =
            let this = Int64.(to_int (min n (of_int twomib_sectors))) in
            let block = Cstruct.sub twomib_empty 0 (this * 512) in
            lwt () = Nbd_lwt_client.write server block (Int64.mul sector 512L) in
            let sector = Int64.(add sector (of_int this)) in
            let n = Int64.(sub n (of_int this)) in
            if progress then P.update p sector;
            if n > 0L then copy sector n else return () in
          copy sector n
        end else return () in
      return (Int64.(add sector (of_int(Element.len x))))
    ) 0L s in
    if progress then Printf.printf "\n%!";

    lwt () = Lwt_unix.close sock in
    return ()

  let stream common filename format output prezeroed progress =
    try
      let filename = require "filename" filename in
      let format = require "format" format in
      let output = require "output" output in

      let thread =      
        lwt t = Vhd_IO.openfile filename in
        lwt s = match format with
          | "raw" ->
            raw t
          | _ -> fail (Failure (Printf.sprintf "%s is an unsupported output format" format)) in
        match output with
        | "human" ->
          stream_human common t s
        | uri ->
          let uri' = Uri.of_string uri in
          begin match Uri.scheme uri' with
          | Some "nbd" ->
            stream_nbd common t s prezeroed progress
          | Some x ->
            fail (Failure (Printf.sprintf "Unknown URI scheme %s" x))
          | None ->
            fail (Failure (Printf.sprintf "Failed to parse URI: %s" uri))
          end in

      Lwt_main.run thread;
      `Ok ()
    with Failure x ->
      `Error(true, x)
end

let get_cmd =
  let doc = "query vhd metadata" in
  let man = [
    `S "DESCRIPTION";
    `P "Look up a particular metadata property by name and print the value."
  ] @ help in
  let filename =
    let doc = Printf.sprintf "Path to the vhd file." in
    Arg.(value & pos 0 (some file) None & info [] ~doc) in
  let key =
    let doc = "Key to query" in
    Arg.(value & pos 1 (some string) None & info [] ~doc) in
  Term.(ret(pure Impl.get $ common_options_t $ filename $ key)),
  Term.info "get" ~sdocs:_common_options ~doc ~man

let info_cmd =
  let doc = "display general information about a vhd" in
  let man = [
    `S "DESCRIPTION";
    `P "Display general information about a vhd, including header and footer fields. This won't directly display block allocation tables or sector bitmaps.";
  ] @ help in
  let filename =
    let doc = Printf.sprintf "Path to the vhd file." in
    Arg.(value & pos 0 (some file) None & info [] ~doc) in
  Term.(ret(pure Impl.info $ common_options_t $ filename)),
  Term.info "info" ~sdocs:_common_options ~doc ~man

let create_cmd =
  let doc = "create a dynamic vhd" in
  let man = [
    `S "DESCRIPTION";
    `P "Create a dynamic vhd (i.e. one which may be sparse). A dynamic vhd may be self-contained or it may have a backing-file or 'parent'.";
  ] @ help in
  let filename =
    let doc = Printf.sprintf "Path to the vhd file to be created." in
    Arg.(value & pos 0 (some string) None & info [] ~doc) in
  let size =
    let doc = Printf.sprintf "Virtual size of the disk." in
    Arg.(value & opt (some string) None & info [ "size" ] ~doc) in
  let parent =
    let doc = Printf.sprintf "Parent image" in
    Arg.(value & opt (some file) None & info [ "parent" ] ~doc) in
  Term.(ret(pure Impl.create $ common_options_t $ filename $ size $ parent)),
  Term.info "create" ~sdocs:_common_options ~doc ~man

let check_cmd =
  let doc = "check the structure of a vhd file" in
  let man = [
    `S "DESCRIPTION";
    `P "Check the structure of a vhd file is valid, print any errors on the console.";
  ] @ help in
  let filename =
    let doc = Printf.sprintf "Path to the vhd to be checked." in
    Arg.(value & pos 0 (some file) None & info [] ~doc) in
  Term.(ret(pure Impl.check $ common_options_t $ filename)),
  Term.info "check" ~sdocs:_common_options ~doc ~man

let stream_cmd =
  let doc = "stream the contents of a vhd disk" in
  let man = [
    `S "DESCRIPTION";
    `P "Stream the contents of a virtual disk defined by the given vhd filename and all of its parents, using the specified format and written to the specified output.";
    `S "FORMATS";
    `P "The default streaming format is \"raw\" which means no encoding. The other supported format is  \"vhd\" where the output is written as a single coalesced vhd.";
    `S "OUTPUTS";
    `P "There are 2 currently defined outputs: \"human\" (the default) where the contents are described as a sequence of I/O operations such as \"insert sector 5 from file x.vhd\". The other defined output is a URL which can take the form:";
    `P "  nbd://host:port/";
    `S "NOTES";
    `P "When transferring a raw format image onto a medium which is completely empty (i.e. full of zeroes) it is possible to optimise the transfer by avoiding writing empty blocks. The default behaviour is to write zeroes, which is always safe. If you know your media is empty then supply the '--prezeroed' argument.";
  ] @ help in
  let format =
    let doc = "Output format" in
    Arg.(value & opt (some string) (Some "raw") & info [ "format" ] ~doc) in
  let filename =
    let doc = Printf.sprintf "Path to the vhd to be streamed." in
    Arg.(value & pos 0 (some file) None & info [] ~doc) in
  let output =
    let doc = "Destination for streamed data." in
    Arg.(value & opt (some string) (Some "human") & info [ "output" ] ~doc) in
  let prezeroed =
    let doc = "Assume the destination is completely empty." in
    Arg.(value & flag & info [ "prezeroed" ] ~doc) in
  let progress =
    let doc = "Display a progress bar." in
    Arg.(value & flag & info ["progress"] ~doc) in
  Term.(ret(pure Impl.stream $ common_options_t $ filename $ format $ output $ prezeroed $ progress)),
  Term.info "stream" ~sdocs:_common_options ~doc ~man


let default_cmd = 
  let doc = "manipulate virtual disks stored in vhd files" in 
  let man = help in
  Term.(ret (pure (fun _ -> `Help (`Pager, None)) $ common_options_t)),
  Term.info "vhd-tool" ~version:"1.0.0" ~sdocs:_common_options ~doc ~man
       
let cmds = [info_cmd; get_cmd; create_cmd; check_cmd; stream_cmd]

let _ =
  match Term.eval_choice default_cmd cmds with 
  | `Error _ -> exit 1
  | _ -> exit 0
