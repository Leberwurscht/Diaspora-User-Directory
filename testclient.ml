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

let socket = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0;;

let sockaddr_to = Unix.ADDR_INET(Unix.inet_addr_of_string "127.0.0.1", 20000);;

Unix.connect socket sockaddr_to;;

let cin = Channel.sys_in_from_fd socket;;
let cout = Channel.sys_out_from_fd socket;;

let data = Server.handle (get_ptree ()) cin cout;;
ZSet.iter ~f:add_number data;

Unix.close socket;;
