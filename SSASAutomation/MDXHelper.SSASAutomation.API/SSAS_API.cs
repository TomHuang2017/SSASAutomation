using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;

namespace MDXHelper.SSASAutomation.API
{
    public class SSAS_API
    {
        #region Populate ssas cube data source view defination
        /// <summary>
        /// Populate ssas cube data source view defination
        /// </summary>
        /// <param name="oledb">oledb connection obj</param>
        /// <param name="dsv">ssas cube data source view obj</param>
        /// <param name="dsvSchemaName">dsv schema name: eg.. OLAP_FACT_SALES</param>
        /// <param name="dsvQueryText">query text of dsv</param>
        /// <param name="dbTableName">depended table name</param>
        /// <param name="tableType"></param>
        /// <param name="db_SchemaName"></param>
        /// <param name="isNamedQuery"></param>
        public static void ADD_TABLE_TO_CUBE_DSV(System.Data.OleDb.OleDbConnection oledb,
            Microsoft.AnalysisServices.DataSourceView dsv,
            String dsvSchemaName,
            String dsvQueryText,
            String dbTableName,
            String tableType = "View",
            String db_SchemaName = "dbo",
            bool isNamedQuery = false)
        {
            System.Data.DataSet schema = dsv.Schema;
            System.Data.OleDb.OleDbCommand select_cmd = new System.Data.OleDb.OleDbCommand();
            select_cmd.CommandText = dsvQueryText;
            select_cmd.Connection = oledb;
            System.Data.OleDb.OleDbDataAdapter adapter = new System.Data.OleDb.OleDbDataAdapter();
            adapter.SelectCommand = select_cmd;
            DataTable[] data_tables = adapter.FillSchema(schema, SchemaType.Mapped, dsvSchemaName);
            DataTable data_table = data_tables[0];
            data_table.ExtendedProperties.Remove("TableType");
            data_table.ExtendedProperties.Add("TableType", tableType);
            if (isNamedQuery)
            {
                data_table.ExtendedProperties.Remove("QueryDefinition");
                data_table.ExtendedProperties.Add("QueryDefinition", dsvQueryText);
            }
            else
            {
                data_table.ExtendedProperties.Remove("DbSchemaName");
                data_table.ExtendedProperties.Add("DbSchemaName", db_SchemaName);
            }
            data_table.ExtendedProperties.Remove("DbTableName");
            data_table.ExtendedProperties.Add("DbTableName", dbTableName);
            data_table.ExtendedProperties.Remove("FriendlyName");
            data_table.ExtendedProperties.Add("FriendlyName", dbTableName);
        }
        #endregion

        #region Add dimension into Cube dataBase, instead of cube
        /// <summary>
        /// Add dimension into Cube dataBase, instead of cube
        /// </summary>
        /// <param name="cubedb">Cube dataBase</param>
        /// <param name="datasourcename">dataSourceName</param>
        /// <param name="dimensionid">dimension id</param>
        /// <param name="dimensionname">dimension id</param>
        /// <param name="dim_type">dimension type, eg..time、regular</param>
        /// <returns></returns>
        public static Microsoft.AnalysisServices.Dimension ADD_DIMENSION(
            Microsoft.AnalysisServices.Database cubedb,
            String datasourcename,
            String dimensionid,
            String dimensionname,
            String dim_type)
        {
            Microsoft.AnalysisServices.Dimension dim = cubedb.Dimensions.FindByName(dimensionname);
            try
            {
                String[] nullvalue = new String[] { "null" };
                if (!nullvalue.Contains(dimensionname))
                {
                    dim = cubedb.Dimensions.Add(dimensionid);
                    dim.Name = dimensionname;
                    dim.Type = Microsoft.AnalysisServices.DimensionType.Regular;
                    if (dim_type.ToLower() == "time")
                    {
                        dim.Type = Microsoft.AnalysisServices.DimensionType.Time;
                    }
                    dim.Source = new Microsoft.AnalysisServices.DataSourceViewBinding(datasourcename);
                    dim.StorageMode = Microsoft.AnalysisServices.DimensionStorageMode.Molap;
                    dim.ProcessingGroup = Microsoft.AnalysisServices.ProcessingGroup.ByAttribute;
                }
                //module_helper.helper.print_message_to_client( u"Added dimension ["+dimensionname+"]")
            }
            finally
            {

            }
            return dim;
        }
        #endregion

        #region Create Column Binding Data Item
        /// <summary>
        /// Create Column Binding Data Item
        /// </summary>
        /// <param name="dsv"></param>
        /// <param name="tableName"></param>
        /// <param name="columnName"></param>
        /// <param name="dataType"></param>
        /// <returns></returns>
        public static Microsoft.AnalysisServices.DataItem CREATE_COLUMN_BINDING_DATA_ITEM(
            Microsoft.AnalysisServices.DataSourceView dsv,
            String tableName,
            String columnName,
            System.Data.OleDb.OleDbType dataType)
        {
            Microsoft.AnalysisServices.DataItem dataItem = null;
            DataTable data_table = dsv.Schema.Tables[tableName];
            if (data_table == null)
            {
                //module_helper.helper.print_message_to_client( "Table "+tableName+" is not existed in current DSV","warning");
            }
            DataColumn dataColumn = data_table.Columns[columnName];
            if (dataColumn == null)
            {
                //module_helper.helper.print_message_to_client( "Column "+columnName+" is not existed in table "+tableName,"warning");
            }
            dataItem = new Microsoft.AnalysisServices.DataItem(tableName, dataColumn.ColumnName);
            if (dataType != null)
            {
                dataItem.DataType = dataType;
            }
            else
            {
                dataItem.DataType = Microsoft.AnalysisServices.OleDbTypeConverter.GetRestrictedOleDbType(dataColumn.DataType);
            }
            return dataItem;
        }
        #endregion

        #region Add Attribute To Dimension
        public static void ADD_ATTRIBUTE_TO_DIMENSION(
            Microsoft.AnalysisServices.DataSourceView cubeDSV,
            Microsoft.AnalysisServices.Dimension dim,
            String tableID,
            String colName,
            String attribID,
            String attribName,
            System.Data.OleDb.OleDbType type,
            Microsoft.AnalysisServices.AttributeUsage usage,
            String nameColumn,
            bool visible = true,
            bool AttHierEnabled = true,
            Microsoft.AnalysisServices.OrderBy orderby = Microsoft.AnalysisServices.OrderBy.Name,
            String attDisplayFolder = "",
            String orderByAttName = null,
            String attType = "Regular",
            String valueColumn = null,
            System.Data.OleDb.OleDbType valueColtype = System.Data.OleDb.OleDbType.Integer)
        {
            Microsoft.AnalysisServices.DimensionAttribute attr = dim.Attributes.FindByName(attribName);
            if (attr == null)
            {
                attr = dim.Attributes.Add(attribID);
                attr.Name = attribName;
                attr.Usage = usage;
                attr.Type = Microsoft.AnalysisServices.AttributeType.Regular;
                attr.AttributeHierarchyEnabled = AttHierEnabled;
                Microsoft.AnalysisServices.DataItem dataItem = CREATE_COLUMN_BINDING_DATA_ITEM(cubeDSV, tableID, colName, type);
                attr.KeyColumns.Add(dataItem);
                attr.KeyColumns[0].DataType = type;
                attr.AttributeHierarchyVisible = visible;
                attr.OrderBy = orderby;
                if (nameColumn != colName && nameColumn != "")
                {
                    Microsoft.AnalysisServices.DataItem nameColDataItem = CREATE_COLUMN_BINDING_DATA_ITEM(cubeDSV, tableID, nameColumn, System.Data.OleDb.OleDbType.WChar);
                    attr.NameColumn = nameColDataItem;
                }
                if (attDisplayFolder != null && attDisplayFolder != "")
                {
                    attr.AttributeHierarchyDisplayFolder = attDisplayFolder;
                }
                if (orderByAttName != null && orderByAttName != "")
                {
                    attr.OrderByAttributeID = orderByAttName;
                }
                if (valueColumn != null && valueColumn != "")
                {
                    Microsoft.AnalysisServices.DataItem valueColDataItem = CREATE_COLUMN_BINDING_DATA_ITEM(cubeDSV, tableID, valueColumn, valueColtype);
                    attr.ValueColumn = valueColDataItem;
                }
                //module_helper.helper.print_message_to_client( "Added attribute ["+attribName+"] to dimension ["+dim.Name+"]")
            }
        }
        #endregion

        #region Add cube dimension
        public static void ADD_CUBE_DIMENSION(
            Microsoft.AnalysisServices.Database cubedb,
            Microsoft.AnalysisServices.Cube cube,
            String dimID,
            String dimension_type,
            String cube_dimName = "",
            bool visible = true)
        {
            Microsoft.AnalysisServices.Dimension dim = cubedb.Dimensions.Find(dimID);
            if (dim == null)
            {
                //   module_helper.helper.print_message_to_client( "Dimension name ["+dimName+"] is not existed in current cube","warning");
            }
            Microsoft.AnalysisServices.CubeDimension cube_dim = cube.Dimensions.Add(dim.ID);
            cube_dim.Visible = visible;
            cube_dim.Name = dim.Name;
            //module_helper.helper.print_message_to_client( "Added dimension ["+cube.Dimensions.FindByName(dim.Name).Name+"] to cube");
        }
        #endregion

        #region Add measure into a measure group

        /// <summary>
        /// Add measure into a measure group
        /// </summary>
        /// <param name="measureGroup"></param>
        /// <param name="tableID"></param>
        /// <param name="columnID"></param>
        /// <param name="measureName"></param>
        /// <param name="measureID"></param>
        /// <param name="displayFolder"></param>
        /// <param name="formatStr"></param>
        /// <param name="aggregationFunction"></param>
        /// <param name="visible"></param>
        /// <param name="sourceColDataType"></param>
        /// <param name="measureDataType"></param>
        public static void ADD_MEASURE_TO_MEASURE_GROUP(
            Microsoft.AnalysisServices.MeasureGroup measureGroup
            , String tableID
            , String columnID
            , String measureName
            , String measureID
            , String displayFolder
            , String formatStr
            , String aggregationFunction
            , bool visible = true
            , String sourceColDataType = "double"
            , String measureDataType = "double")
        {
            Microsoft.AnalysisServices.DataItem source = new Microsoft.AnalysisServices.DataItem();
            source.NullProcessing = Microsoft.AnalysisServices.NullProcessing.Preserve;
            Microsoft.AnalysisServices.Measure measure = new Microsoft.AnalysisServices.Measure(measureName, measureID);
            String aggType = aggregationFunction.ToLower();
            if (aggType == "count*")
            {
                Microsoft.AnalysisServices.RowBinding rowBind = new Microsoft.AnalysisServices.RowBinding();
                rowBind.TableID = tableID;
                measure.AggregateFunction = Microsoft.AnalysisServices.AggregationFunction.Count;
                source.Source = rowBind;
                //source.Source.TableID = tableID;
                measure.Source = source;
            }
            else
            {
                Microsoft.AnalysisServices.ColumnBinding colBind = new Microsoft.AnalysisServices.ColumnBinding();
                colBind.TableID = tableID;
                colBind.ColumnID = columnID;
                source.DataType = SSAS_API_HELPER.GET_SSAS_OLEDB_TYPE_BY_NAME(sourceColDataType);
                source.Source = colBind;
                measure.AggregateFunction = SSAS_API_HELPER.GET_SSAS_AGGREGATION_FUNCTION_BY_NAME(aggType.ToLower());
                if (aggType.ToLower() == "distinctcount")
                {
                    source.NullProcessing = Microsoft.AnalysisServices.NullProcessing.Automatic;
                }
                measure.Source = source;
            }
            measure.DataType = SSAS_API_HELPER.GET_SSAS_MEASURE_DATA_TYPE_BY_NAME(measureDataType);
            String dataType = sourceColDataType.ToLower();
            measure.DisplayFolder = displayFolder;
            //measure.FormatString = formatStr
            measure.Visible = visible;
            Microsoft.AnalysisServices.Measure measureEx = measureGroup.Measures.Find(measureID);
            if (measureEx != null)
            {
                //module_helper.helper.print_message_to_client( "measure ["+measureName+"] exists")
                measureEx.Name = measure.Name;
                measureEx.AggregateFunction = measure.AggregateFunction;
                measureEx.DataType = SSAS_API_HELPER.GET_SSAS_MEASURE_DATA_TYPE_BY_NAME(measureDataType);
                measureEx.DisplayFolder = measure.DisplayFolder;
                measureEx.Visible = measure.Visible;
                measureEx.FormatString = measure.FormatString;
                measureEx.Source = source.Clone();
            }
            else
            {
                //module_helper.helper.print_message_to_client( "Added measure ["+measureName+"] to "+measureGroup.Name)
                measureGroup.Measures.Add(measure);
            }
        }
        #endregion
    }
}
