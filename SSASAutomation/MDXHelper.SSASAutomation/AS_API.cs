using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using Microsoft.AnalysisServices;

namespace MDXHelper.SSASAutomation
{
    internal class AS_API
    {
        #region ADD_TABLE_TO_CUBE_DSV
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
        internal static void ADD_TABLE_TO_CUBE_DSV(IDbConnection oledb,
            DataSourceView dsv,
            String dsvSchemaName,
            String dsvQueryText,
            String dbTableName,
            String tableType,
            String db_SchemaName,
            bool isNamedQuery = false)
        {
            System.Data.DataSet schema = dsv.Schema;
            System.Data.OleDb.OleDbCommand select_cmd = new System.Data.OleDb.OleDbCommand();
            select_cmd.CommandText = dsvQueryText;
            select_cmd.Connection = oledb as System.Data.OleDb.OleDbConnection;
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

        #region ADD_DIMENSION (into cube db)
        /// <summary>
        /// Add dimension into Cube dataBase, instead of cube
        /// </summary>
        /// <param name="cubedb">Cube dataBase</param>
        /// <param name="datasourcename">dataSourceName</param>
        /// <param name="dimensionid">dimension id</param>
        /// <param name="dimensionname">dimension id</param>
        /// <param name="dim_type">dimension type, eg..time、regular</param>
        /// <returns></returns>
        internal static Dimension ADD_DIMENSION(
            DB_SQLHELPER_BASE sqlHelper,
            Database cubedb,
            String datasourcename,
            String dimensionid,
            String dimensionname,
            String dim_type)
        {
            Dimension dim = cubedb.Dimensions.FindByName(dimensionname);
            try
            {
                String[] nullvalue = new String[] { "null" };
                if (!nullvalue.Contains(dimensionname))
                {
                    dim = cubedb.Dimensions.Add(dimensionid);
                    dim.Name = dimensionname;
                    dim.Type = DimensionType.Regular;
                    if (dim_type.ToLower() == "time")
                    {
                        dim.Type = DimensionType.Time;
                    }
                    dim.Source = new DataSourceViewBinding(datasourcename);
                    dim.StorageMode = DimensionStorageMode.Molap;
                    dim.ProcessingGroup = ProcessingGroup.ByAttribute;
                }
                sqlHelper.ADD_MESSAGE_LOG(
                    String.Format("Added dimension [{0}]", dimensionname),
                    MESSAGE_TYPE.DIMENSION, MESSAGE_RESULT_TYPE.Succeed);
            }
            finally
            {
            }
            return dim;
        }
        #endregion

        #region CREATE_COLUMN_BINDING_DATA_ITEM
        /// <summary>
        /// Create Column Binding Data Item
        /// </summary>
        /// <param name="dsv"></param>
        /// <param name="tableName"></param>
        /// <param name="columnName"></param>
        /// <param name="dataType"></param>
        /// <returns></returns>
        internal static DataItem CREATE_COLUMN_BINDING_DATA_ITEM(
            DB_SQLHELPER_BASE sqlHelper,
            DataSourceView dsv,
            String tableName,
            String columnName,
            System.Data.OleDb.OleDbType dataType)
        {
            DataItem dataItem = null;
            DataTable data_table = dsv.Schema.Tables[tableName];
            if (data_table == null)
            {
                sqlHelper.ADD_MESSAGE_LOG(
                    String.Format("Table {0} is not existed in current DSV", tableName),
                    MESSAGE_TYPE.COLUMN_BINDING_DATA_ITEM, MESSAGE_RESULT_TYPE.Warning);
            }
            DataColumn dataColumn = data_table.Columns[columnName];
            if (dataColumn == null)
            {
                sqlHelper.ADD_MESSAGE_LOG(
                    String.Format("Column {0} is not existed in table {1}", columnName, tableName),
                    MESSAGE_TYPE.COLUMN_BINDING_DATA_ITEM, MESSAGE_RESULT_TYPE.Warning);
            }
            dataItem = new DataItem(tableName, dataColumn.ColumnName);
            if (dataType != null)
            {
                dataItem.DataType = dataType;
            }
            else
            {
                dataItem.DataType = OleDbTypeConverter.GetRestrictedOleDbType(dataColumn.DataType);
            }
            return dataItem;
        }
        #endregion

        #region CREATE_DATA_ITEM
        /// <summary>
        /// Create data item
        /// </summary>
        /// <param name="dsv"></param>
        /// <param name="factTableName"></param>
        /// <param name="factFKDimColumnName"></param>
        /// <param name="dataType"></param>
        /// <returns></returns>
        internal static DataItem CREATE_DATA_ITEM(
            DB_SQLHELPER_BASE sqlHelper,
            DataSourceView dsv,
            String factTableName,
            String factFKDimColumnName,
            System.Data.OleDb.OleDbType dataType)
        {
            DataTable data_table = dsv.Schema.Tables[factTableName];
            DataColumn dataColumn = data_table.Columns[factFKDimColumnName];
            if (dataColumn == null)
            {
                sqlHelper.ADD_MESSAGE_LOG(

                    String.Format("Table [{0}] doesn't have column [{0}]", factTableName, factFKDimColumnName),
                    MESSAGE_TYPE.COLUMN_BINDING_DATA_ITEM, MESSAGE_RESULT_TYPE.Error);

            }
            DataItem dataItem = new DataItem(factTableName, dataColumn.ColumnName);
            if (dataType != null)
            {
                dataItem.DataType = dataType;
            }
            else
            {
                dataItem.DataType = OleDbTypeConverter.GetRestrictedOleDbType(dataColumn.DataType);
            }
            return dataItem;
        }
        #endregion

        #region ADD_ATTRIBUTE_TO_DIMENSION
        /// <summary>
        /// Add Attribute To Dimension
        /// </summary>
        /// <param name="cubeDSV"></param>
        /// <param name="dim"></param>
        /// <param name="tableID"></param>
        /// <param name="colName"></param>
        /// <param name="attribID"></param>
        /// <param name="attribName"></param>
        /// <param name="type"></param>
        /// <param name="usage"></param>
        /// <param name="nameColumn"></param>
        /// <param name="visible"></param>
        /// <param name="AttHierEnabled"></param>
        /// <param name="orderby"></param>
        /// <param name="attDisplayFolder"></param>
        /// <param name="orderByAttName"></param>
        /// <param name="attType"></param>
        /// <param name="valueColumn"></param>
        /// <param name="valueColtype"></param>
        internal static void ADD_ATTRIBUTE_TO_DIMENSION(
            DB_SQLHELPER_BASE sqlHelper,
            DataSourceView cubeDSV,
            Dimension dim,
            String tableID,
            String colName,
            String attribID,
            String attribName,
            System.Data.OleDb.OleDbType type,
            AttributeUsage usage,
            String nameColumn,
            bool visible = true,
            bool AttHierEnabled = true,
            OrderBy orderby = OrderBy.Name,
            String attDisplayFolder = "",
            String orderByAttName = null,
            String attType = "Regular",
            String valueColumn = null,
            System.Data.OleDb.OleDbType valueColtype = System.Data.OleDb.OleDbType.Integer)
        {
            DimensionAttribute attr = dim.Attributes.FindByName(attribName);
            if (attr == null)
            {
                attr = dim.Attributes.Add(attribID);
                attr.Name = attribName;
                attr.Usage = usage;
                attr.Type = AttributeType.Regular;
                attr.AttributeHierarchyEnabled = AttHierEnabled;
                DataItem dataItem = CREATE_COLUMN_BINDING_DATA_ITEM(sqlHelper,cubeDSV, tableID, colName, type);
                attr.KeyColumns.Add(dataItem);
                attr.KeyColumns[0].DataType = type;
                attr.AttributeHierarchyVisible = visible;
                attr.OrderBy = orderby;
                if (nameColumn != colName && nameColumn != "")
                {
                    DataItem nameColDataItem = CREATE_COLUMN_BINDING_DATA_ITEM(sqlHelper,cubeDSV, tableID, nameColumn, System.Data.OleDb.OleDbType.WChar);
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
                    DataItem valueColDataItem = CREATE_COLUMN_BINDING_DATA_ITEM(sqlHelper,cubeDSV, tableID, valueColumn, valueColtype);
                    attr.ValueColumn = valueColDataItem;
                }
                sqlHelper.ADD_MESSAGE_LOG(

                    String.Format("Added attribute [{0}] to dimension [{1}]", attribName, dim.Name),
                    MESSAGE_TYPE.ATTRIBUTE, MESSAGE_RESULT_TYPE.Succeed);
            }
        }
        #endregion

        #region ADD_ATTRIBUTE_RELATIONSHIP
        /// <summary>
        /// Add relation ship to attribute
        /// </summary>
        /// <param name="dim"></param>
        /// <param name="basedAttributeID"></param>
        /// <param name="relatedAttributeID"></param>
        /// <param name="attributeRelationShipType"></param>
        /// <param name="relationShipName"></param>
        internal static void ADD_ATTRIBUTE_RELATIONSHIP(
            DB_SQLHELPER_BASE sqlHelper,
            Dimension dim,
            String basedAttributeID,
            String relatedAttributeID,
            RelationshipType attributeRelationShipType = RelationshipType.Flexible,
            String relationShipName = null)
        {
            DimensionAttribute attr = dim.Attributes.Find(basedAttributeID);
            DimensionAttribute relatedAttr = dim.Attributes.Find(relatedAttributeID);
            if (relationShipName == null)
            {
                relationShipName = relatedAttr.Name;
            }
            AttributeRelationship relationship = new AttributeRelationship();
            relationship.Attribute = attr;
            relationship.Name = relationShipName;
            relationship.AttributeID = relatedAttributeID;
            if (attributeRelationShipType != null)
            {
                relationship.RelationshipType = attributeRelationShipType;
            }
            else
            {
                sqlHelper.ADD_MESSAGE_LOG(
                    String.Format("A None RelationShipType is passed between [{0} and [{1}]", basedAttributeID, relatedAttributeID),
                        MESSAGE_TYPE.ATTRIBUTE_RELATIONSHIP, MESSAGE_RESULT_TYPE.Warning);
            }
            if (!attr.AttributeRelationships.Contains(relatedAttributeID))
            {
                attr.AttributeRelationships.Add(relationship);
            }
        }
        #endregion

        #region ADD_ATTRIBUTE_HIERACHIES
        /// <summary>
        /// Add attribute hierachies
        /// </summary>
        /// <param name="dim"></param>
        /// <param name="hierarchyName"></param>
        /// <param name="levelName"></param>
        /// <param name="sourceAttributeID"></param>
        internal static void ADD_ATTRIBUTE_HIERACHIES(
            DB_SQLHELPER_BASE sqlHelper,
            Dimension dim,
            String hierarchyName,
            String levelName,
            String sourceAttributeID)
        {
            if (dim.Hierarchies.FindByName(hierarchyName) == null)
            {
                dim.Hierarchies.Add(hierarchyName);
            }
            Hierarchy hierarchy = dim.Hierarchies.FindByName(hierarchyName);
            if (hierarchy.Levels.FindByName(levelName) != null)
            {
                sqlHelper.ADD_MESSAGE_LOG(

                    String.Format("Level [{0}] already existed for hierarchy [{0}] ", levelName, hierarchyName),
                    MESSAGE_TYPE.HIERARCHIES, MESSAGE_RESULT_TYPE.Warning);
            }
            Level level = hierarchy.Levels.Add(levelName);
            level.SourceAttributeID = sourceAttributeID;
        }
        #endregion

        #region ADD_CUBE_DIMENSION
        /// <summary>
        /// Add cube dimension (instead of cube db)
        /// </summary>
        /// <param name="cubedb"></param>
        /// <param name="cube"></param>
        /// <param name="dimID"></param>
        /// <param name="dimension_type"></param>
        /// <param name="cube_dimName"></param>
        /// <param name="visible"></param>
        internal static void ADD_CUBE_DIMENSION(
            DB_SQLHELPER_BASE sqlHelper,
            Database cubedb,
            Cube cube,
            String dimID,
            String dimension_type,
            String cube_dimName = "",
            bool visible = true)
        {
            Dimension dim = cubedb.Dimensions.Find(dimID);
            if (dim == null)
            {
                sqlHelper.ADD_MESSAGE_LOG(

                    String.Format("Detected dimension name [{0}] is not existed in current cube db", cube_dimName),
                    MESSAGE_TYPE.DIMENSION, MESSAGE_RESULT_TYPE.Warning);
            }
            CubeDimension cube_dim = cube.Dimensions.Add(dim.ID);
            cube_dim.Visible = visible;
            cube_dim.Name = dim.Name;
            sqlHelper.ADD_MESSAGE_LOG(
                String.Format("Added dimension [{0}] into cube (instead of cube db)", cube.Dimensions.FindByName(dim.Name).Name),
                MESSAGE_TYPE.DIMENSION, MESSAGE_RESULT_TYPE.Succeed);
        }
        #endregion

        #region ADD_MEASURE_GROUP
        /// <summary>
        /// add measure group into cube
        /// </summary>
        /// <param name="cube"> cube (instead of cube db)</param>
        /// <param name="measureGroupName"></param>
        /// <param name="measureGroupID"></param>
        /// <param name="isRealTime"></param>
        /// <param name="keyNotFoundAction"></param>
        /// <param name="dropIfExisted"></param>
        /// <returns>measure group</returns>
        internal static MeasureGroup ADD_MEASURE_GROUP(
            DB_SQLHELPER_BASE sqlHelper,
            Cube cube,
            String measureGroupName,
            String measureGroupID,
            int isRealTime,
            String keyNotFoundAction,
            bool dropIfExisted = false)
        {
            MeasureGroup measure_group = cube.MeasureGroups.Find(measureGroupID);
            if (measure_group != null && dropIfExisted)
            {
                measure_group.Drop();
            }
            measure_group = cube.MeasureGroups.Add(measureGroupID);
            measure_group.Name = measureGroupName;
            if (isRealTime == '0')
            {
                measure_group.StorageMode = StorageMode.Molap;
            }
            else
            {
                measure_group.StorageMode = StorageMode.Rolap;
            }
            measure_group.Type = MeasureGroupType.Regular;
            if (keyNotFoundAction != "0")
            {
                ErrorConfiguration error_configuration = new ErrorConfiguration();
                switch (keyNotFoundAction.ToLower())
                {
                    case "ignoreerror":
                        error_configuration.KeyNotFound = ErrorOption.IgnoreError;
                        break;
                    case "reportandcontinue":
                        error_configuration.KeyNotFound = ErrorOption.ReportAndContinue;
                        break;
                    case "reportandstop":
                        error_configuration.KeyNotFound = ErrorOption.ReportAndStop;
                        break;
                    default:
                        error_configuration.KeyNotFound = ErrorOption.IgnoreError;
                        break;
                }
                measure_group.ErrorConfiguration = error_configuration;
            }
            sqlHelper.ADD_MESSAGE_LOG(
                String.Format("Added measure group {0} into cube", measureGroupName),
                MESSAGE_TYPE.MEASURE_GROUP, MESSAGE_RESULT_TYPE.Succeed);
            return measure_group;
        }
        #endregion

        #region ADD_MEASURE_TO_MEASURE_GROUP

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
        internal static void ADD_MEASURE_TO_MEASURE_GROUP(
            DB_SQLHELPER_BASE sqlHelper
            , MeasureGroup measureGroup
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
            source.NullProcessing = NullProcessing.Preserve;
            Measure measure = new Measure(measureName, measureID);
            String aggType = aggregationFunction.ToLower();
            measure.DataType = AS_API_HELPER.GET_SSAS_MEASURE_DATA_TYPE_BY_NAME(measureDataType);
            if (aggType == "count*")
            {
                RowBinding rowBind = new RowBinding();
                rowBind.TableID = tableID;
                measure.AggregateFunction = AggregationFunction.Count;
                source.Source = rowBind;
                measure.Source = source;
                measure.DataType = MeasureDataType.Integer;
                //source.DataType = AS_API_HELPER.GET_SSAS_OLEDB_TYPE_BY_NAME(sourceColDataType);
            }
            else
            {
                ColumnBinding colBind = new ColumnBinding();
                colBind.TableID = tableID;
                colBind.ColumnID = columnID;
                source.DataType = AS_API_HELPER.GET_SSAS_OLEDB_TYPE_BY_NAME(sourceColDataType);
                source.Source = colBind;
                measure.AggregateFunction = AS_API_HELPER.GET_SSAS_AGGREGATION_FUNCTION_BY_NAME(aggType.ToLower());
                if (aggType.ToLower() == "distinctcount")
                {
                    source.NullProcessing = NullProcessing.Automatic;
                    source.DataType = AS_API_HELPER.GET_SSAS_OLEDB_TYPE_BY_NAME("integer");
                    measure.DataType = MeasureDataType.Integer;
                }
                measure.Source = source;
            }
            String dataType = sourceColDataType.ToLower();
            measure.DisplayFolder = displayFolder;
            //measure.FormatString = formatStr
            measure.Visible = visible;
            Measure measureEx = measureGroup.Measures.Find(measureID);
            if (measureEx != null)
            {
                sqlHelper.ADD_MESSAGE_LOG(

                String.Format("measure {0} exists", measureName),
                MESSAGE_TYPE.MEASURES, MESSAGE_RESULT_TYPE.Warning);
                measureEx.Name = measure.Name;
                measureEx.AggregateFunction = measure.AggregateFunction;
                measureEx.DataType = AS_API_HELPER.GET_SSAS_MEASURE_DATA_TYPE_BY_NAME(measureDataType);
                measureEx.DisplayFolder = measure.DisplayFolder;
                measureEx.Visible = measure.Visible;
                measureEx.FormatString = measure.FormatString;
                measureEx.Source = source.Clone();
            }
            else
            {
                sqlHelper.ADD_MESSAGE_LOG(
                String.Format("Added measure {0} into measure group {1}", measureName, measureGroup.Name),
                MESSAGE_TYPE.MEASURES, MESSAGE_RESULT_TYPE.Succeed);
                measureGroup.Measures.Add(measure);
            }
        }
        #endregion

        #region ADD_DIM_USAGE_REGULAR_RELATIONSHIP
        /// <summary>
        /// Add regular dim usage
        /// </summary>
        /// <param name="cube"></param>
        /// <param name="measureGroup"></param>
        /// <param name="factDataItem"></param>
        /// <param name="dimID"></param>
        /// <param name="dimAttrId"></param>
        /// <returns>Regular dim usage</returns>
        internal static RegularMeasureGroupDimension ADD_DIM_USAGE_REGULAR_RELATIONSHIP(
            DB_SQLHELPER_BASE sqlHelper,
            Cube cube,
            MeasureGroup measureGroup,
            DataItem factDataItem,
            String dimID,
            String dimAttrId)
        {
            RegularMeasureGroupDimension regMgDim = null;
            CubeDimension curDim = cube.Dimensions.Find(dimID);
            if (curDim == null)
            {
                sqlHelper.ADD_MESSAGE_LOG(
                String.Format("Cann't find dimension {0}", dimID),
                MESSAGE_TYPE.DIM_USAGE_REGULAR, MESSAGE_RESULT_TYPE.Error);
            }

            regMgDim = new RegularMeasureGroupDimension(curDim.Name);
            regMgDim.CubeDimensionID = dimID;
            measureGroup.Dimensions.Add(regMgDim);
            MeasureGroupAttribute mgAttr = regMgDim.Attributes.Add(dimAttrId);
            mgAttr.Type = MeasureGroupAttributeType.Granularity;
            mgAttr.KeyColumns.Add(factDataItem);
            return regMgDim;
        }
        #endregion

        #region ADD_DIM_USAGE_REFERENCE_RELATIONSHIP
        /// <summary>
        /// Add reference dim usage 
        /// </summary>
        /// <param name="measureGroup"></param>
        /// <param name="referenceDimID"></param>
        /// <param name="referenceDimAttrID"></param>
        /// <param name="interDimID"></param>
        /// <param name="interDimAttrID"></param>
        internal static void ADD_DIM_USAGE_REFERENCE_RELATIONSHIP(
            MeasureGroup measureGroup,
            String referenceDimID,
            String referenceDimAttrID,
            String interDimID,
            String interDimAttrID)
        {
            MeasureGroupDimension regDim = measureGroup.Dimensions.Find(referenceDimID);
            if (regDim != null)
            {
                measureGroup.Dimensions.Remove(regDim);
            }
            ReferenceMeasureGroupDimension regMgDim = new ReferenceMeasureGroupDimension();
            regMgDim.CubeDimensionID = referenceDimID;
            regMgDim.IntermediateCubeDimensionID = interDimID;
            MeasureGroupAttribute mgAttr = regMgDim.Attributes.Add(referenceDimAttrID);
            mgAttr.Type = MeasureGroupAttributeType.Granularity;
            regMgDim.IntermediateGranularityAttributeID = interDimAttrID;
            regMgDim.Materialization = ReferenceDimensionMaterialization.Regular;
            measureGroup.Dimensions.Add(regMgDim);
        }
        #endregion

        #region ADD_DIM_USAGE_MANY_RELATIONSHIP
        /// <summary>
        /// Add many to many dim_usage
        /// </summary>
        /// <param name="measureGroup"></param>
        /// <param name="internalMGID"></param>
        /// <param name="DimID"></param>
        internal static void ADD_DIM_USAGE_MANY_RELATIONSHIP(
            MeasureGroup measureGroup,
            String internalMGID,
            String DimID)
        {
            ManyToManyMeasureGroupDimension manytomanyMgDim = new ManyToManyMeasureGroupDimension();
            manytomanyMgDim.CubeDimensionID = DimID;
            manytomanyMgDim.MeasureGroupID = internalMGID;
            measureGroup.Dimensions.Add(manytomanyMgDim);
        }
        #endregion

        #region ADD_DIM_USAGE_FACT_RELATIONSHIP
        /// <summary>
        /// Add fact dim usage
        /// </summary>
        /// <param name="measureGroup"></param>
        /// <param name="GranularityAttrID"></param>
        /// <param name="DimID"></param>
        internal static void ADD_DIM_USAGE_FACT_RELATIONSHIP(
            MeasureGroup measureGroup,
            String GranularityAttrID,
            String DimID)
        {
            DegenerateMeasureGroupDimension factMgDim = new DegenerateMeasureGroupDimension();
            factMgDim.CubeDimensionID = DimID;
            MeasureGroupAttribute mgAttr = factMgDim.Attributes.Add(GranularityAttrID);
            mgAttr.Type = MeasureGroupAttributeType.Granularity;
            measureGroup.Dimensions.Add(factMgDim);
        }
        #endregion

        #region CREATE_MOLAP_PARTITION
        /// <summary>
        /// Create molap partition 
        /// </summary>
        /// <param name="measureGroup"></param>
        /// <param name="datasourceName"></param>
        /// <param name="partitionid"></param>
        /// <param name="MGdsvTableName"></param>
        /// <param name="filter_string"></param>
        /// <param name="aggregation_design_id"></param>
        /// <param name="is_real_time"></param>
        /// <param name="depended_fact_table"></param>
        internal static void CREATE_MOLAP_PARTITION(
            DB_SQLHELPER_BASE sqlHelper,
            MeasureGroup measureGroup,
            String datasourceName,
            String partitionid,
            String MGdsvTableName,
            String filter_string,
            String aggregation_design_id,
            int is_rolap_mg,
            String depended_fact_table)
        {
            Partition part = measureGroup.Partitions.FindByName(partitionid);
            if (part != null)
            {
                sqlHelper.ADD_MESSAGE_LOG(
                    String.Format("Drop Partition {0}", partitionid),
                    MESSAGE_TYPE.DIMENSION, MESSAGE_RESULT_TYPE.Succeed);
                part.Drop(DropOptions.AlterOrDeleteDependents);
            }

            part = measureGroup.Partitions.Add(partitionid);
            part.ID = partitionid;
            part.Name = partitionid;
            part.Source = new QueryBinding(datasourceName, "SELECT * FROM " + MGdsvTableName + " WHERE 1=1 " + filter_string);
            if (is_rolap_mg.ToString() == "1")
            {
                part.StorageMode = StorageMode.Rolap;
                part.CurrentStorageMode = StorageMode.Rolap;
                ProactiveCachingTablesBinding tables_binding = new ProactiveCachingTablesBinding();
                tables_binding.NotificationTechnique = NotificationTechnique.Server;
                TableNotification table_notification = new TableNotification(depended_fact_table, CONFIGURATION_HELPER.GET_METADATA_PROPERTY("db_table_schema_name"));
                tables_binding.TableNotifications.Add(table_notification);

                ProactiveCaching proactive_caching = new ProactiveCaching();
                proactive_caching.OnlineMode = ProactiveCachingOnlineMode.Immediate;
                proactive_caching.AggregationStorage = ProactiveCachingAggregationStorage.MolapOnly;
                proactive_caching.Enabled = true;
                proactive_caching.Source = tables_binding;
                System.TimeSpan SilenceInterval_time = new System.TimeSpan(0, 0, 1);
                proactive_caching.SilenceInterval = SilenceInterval_time.Negate();
                proactive_caching.SilenceOverrideInterval = proactive_caching.SilenceInterval;
                proactive_caching.ForceRebuildInterval = proactive_caching.SilenceInterval;
                proactive_caching.Latency = System.TimeSpan.Zero;
                part.ProactiveCaching = proactive_caching;
            }
            else
            {
                part.StorageMode = StorageMode.Molap;
            }
            part.ProcessingMode = ProcessingMode.Regular;
            if (aggregation_design_id != null)
            {
                part.AggregationDesignID = aggregation_design_id.ToString();
            }
        }
        #endregion

        #region REMOVE_MEASURE_GROUPS
        /// <summary>
        /// Remove measure group
        /// </summary>
        /// <param name="cube"></param>
        /// <param name="mgID"></param>
        internal static void REMOVE_MEASURE_GROUPS(DB_SQLHELPER_BASE sqlHelper, Cube cube, String mgID)
        {
            MeasureGroup mg = cube.MeasureGroups.Find(mgID);
            if (mg != null)
            {
                cube.MeasureGroups.Remove(mg);
                sqlHelper.ADD_MESSAGE_LOG(

                    String.Format("Deleted [{0}] measure group", mgID),
                    MESSAGE_TYPE.MEASURE_GROUP, MESSAGE_RESULT_TYPE.Succeed);
            }
        }
        #endregion

        #region REMOVE_DIMENSIONS
        /// <summary>
        /// Remove dimension
        /// </summary>
        /// <param name="cubedb"></param>
        /// <param name="cube"></param>
        /// <param name="dimID"></param>
        internal static void REMOVE_DIMENSIONS(
            DB_SQLHELPER_BASE sqlHelper,
            Database cubedb,
            Cube cube,
            String dimID)
        {
            CubeDimension cube_dim = cube.Dimensions.Find(dimID);
            if (cube_dim != null)
            {
                cube.Dimensions.Remove(cube_dim);
            }

            Dimension dim = cubedb.Dimensions.Find(dimID);
            if (dim != null)
            {
                cubedb.Dimensions.Remove(dim);
                sqlHelper.ADD_MESSAGE_LOG(
                    String.Format("Delete [{0}] dimension", dim.Name),
                    MESSAGE_TYPE.DIMENSION, MESSAGE_RESULT_TYPE.Succeed);
            }
        }
        #endregion
    }
}