package mysql_phoenix;
public enum relation1 {

	
		// ->
		PRECEDES(">"),
		// <-
		FOLLOWS("<"),
		// ||
		PARALLEL("|"),
		// #
		NOT_CONNECTED("#"),
		//casuality
		CASUALITY("&");
		relation1(String symbol){
			this.sym=symbol;
		}
		private final String sym;
		public String symbol(){return sym;}
	}
