(************************************************************************)
(* This file is part of SKS.  SKS is free software; you can
   redistribute it and/or modify it under the terms of the GNU General
   Public License as published by the Free Software Foundation; either
   version 2 of the License, or (at your option) any later version.

   This program is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307
   USA *)
(***********************************************************************)

(** Executable: Builds up the key database from a multi-file database dump.
  dump files are taken from the command-line 
*)
module F(M:sig end) =
struct
  open StdLabels
  open MoreLabels
  open Printf
  open Arg
  open Common
  module Set = PSet.Set
  open Packet
  let settings = {
    Keydb.withtxn = false;
    Keydb.cache_bytes = !Settings.cache_bytes;
    Keydb.pagesize = !Settings.pagesize;
    Keydb.dbdir = Lazy.force Settings.dbdir;
    Keydb.dumpdir = Lazy.force Settings.dumpdir;
  }

  module Keydb = Keydb.Safe

  let n = match !Settings.n with 0 -> 1 | x -> x
  let fnames = !Settings.anonlist

  let rec get_keys_rec nextkey partial = match nextkey () with 
      Some key -> 
	(try
	   let ckey = Fixkey.canonicalize key in 
	   get_keys_rec nextkey (ckey::partial)
	 with
	     Fixkey.Bad_key -> get_keys_rec nextkey partial
	)
    | None -> partial
	
  let get_keys nextkey = get_keys_rec nextkey []

  let timestr sec = 
    sprintf "%.2f min" (sec /. 60.)
      
  let rec nsplit n list = match n with
      0 -> ([],list)
    | n -> match list with
	  [] -> ([],[])
	| hd::tl -> 
	    let (beginning,ending) = nsplit (n-1) tl in
	    (hd::beginning,ending)

  let rec batch_iter ~f n list = 
    match nsplit n list with
	([],_) -> ()
      | (firstn,rest) -> f firstn; batch_iter ~f n rest

  let get_keys_fname fname start = 
    let cin = new Channel.sys_in_channel (open_in fname) in
    protect 
      ~f:(fun () -> 
	    let nextkey = Key.next_of_channel cin in
	    get_keys_rec nextkey start
	 )
      ~finally:(fun () -> cin#close)

  let get_keys_multi flist = 
    List.fold_left ~f:(fun keys fname -> get_keys_fname fname keys)
      flist ~init:[]

  let dbtimer = MTimer.create ()
  let timer = MTimer.create ()
  let run () = 
    set_logfile "build";

    if Sys.file_exists (Lazy.force Settings.dbdir) then (
      printf "KeyDB directory already exists.  Exiting.\n";
      exit (-1)
    );
    Unix.mkdir (Lazy.force Settings.dbdir) 0o700; 

    Keydb.open_dbs settings;
    Keydb.set_meta ~key:"filters" ~data:"yminsky.dedup";
    
    protect 
      ~f:(fun () ->
	    batch_iter n fnames 
	    ~f:(fun fnames ->
		  MTimer.start timer;
		  printf "Loading keys..."; flush stdout;
		  let keys = get_keys_multi fnames in
		  printf "done\n"; flush stdout;
		  MTimer.start dbtimer;
		  Keydb.add_keys keys;
		  MTimer.stop dbtimer;
		  MTimer.stop timer;
		  printf "DB time:  %s.  Total time: %s.\n" 
		    (timestr (MTimer.read dbtimer)) 
		    (timestr (MTimer.read timer)); 
		  flush stdout;
	       )
	 )
      ~finally:(fun () -> Keydb.close_dbs ())

end
