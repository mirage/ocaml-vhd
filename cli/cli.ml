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

open Vhd
open Vhd_lwt

let make_sector byte =
  let sector = Cstruct.create 512 in
  for i = 0 to 511 do
    Cstruct.set_uint8 sector i byte
  done;
  sector

let round_up_to_2mb_block size = 
  let newsize = Int64.mul 2097152L 
    (Int64.div (Int64.add 2097151L size) 2097152L) in
  newsize 

let main () =
  match Sys.argv.(1) with
    | "create" ->
        lwt vhd = create_new_dynamic "test.vhd" 4194304L (Uuidm.create `V4) () in
        lwt () = Vhd_IO.write vhd in
        let sector = make_sector (int_of_char 'A') in
        Vhd_IO.write_sector vhd 0L sector
    | "creatediff" ->
        lwt vhd = create_new_difference "test2.vhd" Sys.argv.(2) (Uuidm.create `V4) () in
	Vhd_IO.write vhd
    | "check" ->
        lwt vhd = Vhd_IO.openfile Sys.argv.(2) in
        Vhd.check_overlapping_blocks vhd;
        Lwt.return ()
    | "stream" ->
        lwt vhd = Vhd_IO.openfile Sys.argv.(2) in
        lwt s = raw vhd in
        iter (fun x -> Printf.printf "%s\n" (Element.to_string x); return ()) s
    | "makefromfile" ->
	    let file = Sys.argv.(2) in
	    lwt filesize = 
	       (lwt st = Lwt_unix.LargeFile.stat file in
		   Printf.printf "st_size: %Ld\n" (st.Lwt_unix.LargeFile.st_size);
		   Lwt.return st.Lwt_unix.LargeFile.st_size) 
        in
        let size = round_up_to_2mb_block 	  
	       (try 
				Int64.of_string Sys.argv.(3)
			with 
				| _ ->
					filesize)
		in
		Printf.printf "size=%Ld\n" size;
		Printf.printf "filesize=%Ld\n" size;
		lwt vhd = create_new_dynamic (file^".vhd") size  (Uuidm.create `V4) () in
        lwt () = Vhd_IO.write vhd in
	    lwt fd = Lwt_unix.openfile file [Unix.O_RDWR] 0o644  in
        let mmap = Cstruct.of_bigarray (Lwt_bytes.map_file ~fd:(Lwt_unix.unix_file_descr fd) ~shared:true ()) in
        let allzeros = make_sector 0 in
		let max = Int64.div filesize 512L in
		let rec doit i =
			if i=max 
            then Lwt.return () 
            else 
				lwt input = really_read mmap (Int64.mul i 512L) 512 in
	            lwt () = 
                    if input<>allzeros then
				        Vhd_IO.write_sector vhd i input
                    else Lwt.return () 
                in
                lwt () = doit (Int64.add 1L i) in
                Lwt.return ()
        in
        lwt () = doit 0L in 
        Lwt.return ()
   | _ -> Printf.fprintf stderr "Unknown command";
		Lwt.return ()

let   _ =
	Lwt_main.run (main ())
