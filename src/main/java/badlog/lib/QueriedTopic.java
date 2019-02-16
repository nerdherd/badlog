package badlog.lib;

import java.util.Optional;
import java.util.function.Supplier;

class QueriedTopic extends Topic {
	
	private String name;
	private Supplier<String> supplier;
	
	private Optional<String> value;

	public QueriedTopic(String name, Supplier<String> supplier) {
		this.name = name;
		this.supplier = supplier;
		
	}
	
	public String getName() {
		return name;
	}
	
	public void refreshValue() {
		value = Optional.of(supplier.get());
	}

	public String getValue() {
		return value.get();
	}
}
