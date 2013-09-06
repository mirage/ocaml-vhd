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
        let size = Int64.of_string size in
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

  let stream_english common filename =
    lwt t = Vhd_IO.openfile filename in
    lwt s = raw t in
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
 
  let stream common filename format =
    try
      let filename = require "filename" filename in
      let format = require "format" format in
      let t = match format with
        | "english" -> stream_english common filename
        | _ -> failwith (Printf.sprintf "%s is an unsupported output format" format) in
      Lwt_main.run t;
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
    `P "Stream the contents of a virtual disk defined by the given vhd filename and all of its parents. The default streaming format is \"english\" where the contents are described as a sequence of I/O operations such as \"insert sector 5 from file x.vhd\". Other streaming formats are: \"raw\" and \"vhd\".";
  ] @ help in
  let format =
    let doc = "Output format" in
    Arg.(value & opt (some string) (Some "english") & info [ "format" ] ~doc) in
  let filename =
    let doc = Printf.sprintf "Path to the vhd to be streamed." in
    Arg.(value & pos 0 (some file) None & info [] ~doc) in
  Term.(ret(pure Impl.stream $ common_options_t $ filename $ format)),
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
