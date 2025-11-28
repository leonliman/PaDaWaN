package de.uniwue.dw.query.model.result;

import java.util.List;

import de.uniwue.dw.query.model.exception.QueryException;

public interface IPostProcessor {

  List<List<ResultCellData>> doPostProcessing(List<ResultCellData> cellData)
          throws NumberFormatException, QueryException;

}
