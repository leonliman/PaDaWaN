package de.uniwue.dw.imports.configured.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ConfigDataXML extends ConfigData {

	public String pidRegex;
	
	public String caseRegex;
	
	public String docRegex;
	
	public String idCSVFile;
	
	public HashMap<String, ConfigAcceptedID> acceptedExtID = new HashMap<String, ConfigAcceptedID>();

	public List<String> rejectedExtID = new ArrayList<String>();
	
  public ConfigDataXML(ConfigStructureElem aParent) {
    super(aParent);
  }

}
