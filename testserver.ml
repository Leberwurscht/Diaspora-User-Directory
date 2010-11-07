open PTreeDB;;

module ZSet = ZZp.Set;;

let settings = { (* copied from reconserver.ml *)
	mbar = !Settings.mbar;
	bitquantum = !Settings.bitquantum;
	treetype = (if !Settings.transactions
		then `transactional
		else if !Settings.disk_ptree 
		then `ondisk else `inmem);
	max_nodes = !Settings.max_ptree_nodes;
	dbdir = Lazy.force Settings.ptree_dbdir;
	cache_bytes = !Settings.ptree_cache_bytes;
	pagesize = !Settings.ptree_pagesize;
};;

init_db settings;;
init_ptree settings;;

let add_number number =
	Printf.printf "got %s\n" (Number.to_string (ZZp.to_number number));
	let txn = new_txnopt () in
		PTree.insert (get_ptree ()) txn number;
		PTree.clean txn (get_ptree ());
		commit_txnopt txn

let unixsockaddr = "server.ocaml2py.sock";;
let () = try Unix.unlink unixsockaddr with Unix.Unix_error _ -> ();;
let addr = Unix.ADDR_UNIX unixsockaddr;;
let sock = Eventloop.create_sock addr;;

let timeout = !Settings.reconciliation_config_timeout;;

let test addr cin cout =
	let cin = (new Channel.sys_in_channel cin)
	and cout = (new Channel.sys_out_channel cout) in
		let data = Client.handle (get_ptree ()) cin cout in
		ZSet.iter ~f:add_number data;

	Common.plerror 1 "talked with %s" (ReconMessages.sockaddr_to_string addr);
	[];;

Eventloop.evloop [] [sock, Eventloop.make_th ~name:"test" ~cb:test ~timeout:timeout];;
