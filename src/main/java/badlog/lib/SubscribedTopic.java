package badlog.lib;

import java.util.Optional;

class SubscribedTopic extends Topic {

	private String name;
	private DataInferMode inferMode;

	private String value = BadLog.DEFAULT_DATA;

	public SubscribedTopic(String name, DataInferMode inferMode) {
		this.name = name;
		this.inferMode = inferMode;
	}

	public String getValue() {
		return value;
	}

	public String getName() {
		return name;
	}

	public void handlePublishedData(Optional<String> data) {
		switch(inferMode) {
		case DEFAULT:
			value = data.orElse(BadLog.DEFAULT_DATA);
			break;
		case LAST:
			if(data.isPresent())
				value = data.get();
			break;
		}
	}
}
