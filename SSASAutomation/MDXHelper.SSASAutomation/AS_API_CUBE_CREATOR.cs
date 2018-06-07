using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AnalysisServices;
using System.Data;
using System.IO;

namespace MDXHelper.SSASAutomation
{
    public class AS_API_CUBE_CREATOR
    {

        #region CREATE_SSAS_BASE_CUBE
        /// <summary>
        /// Create ssas base cube by calling SSAS API
        /// </summary>
        /// <param name="cubeServer">Cube server</param>
        /// <param name="cubeDBName">Cube data base name</param>
        /// <param name="cubeXmla">Base cube xmla</param>
        public void CREATE_SSAS_BASE_CUBE(
            DB_SQLHELPER_BASE sqlHelper,
            Server cubeServer, 
            String cubeDBName, 
            String cubeName,
            String dwConnectionString
        )
        {
            String SSASConfigurationPath = CONFIGURATION_HELPER.BASIC_CONFIGURATION_FOLDER + @"\SSASConfiguration\BaseCubeXMLA.xml";
            String SSASConfiguration = "";

            System.Security.Principal.NTAccount _Everyone_Account = new System.Security.Principal.NTAccount("Everyone");
            System.Security.Principal.SecurityIdentifier _SecurityIdentifier = (System.Security.Principal.SecurityIdentifier)_Everyone_Account.Translate(typeof(System.Security.Principal.SecurityIdentifier));
            String sidString = _SecurityIdentifier.ToString();

            SSASConfiguration = System.IO.File.ReadAllText(SSASConfigurationPath);
            SSASConfiguration = SSASConfiguration
                .Replace("$(dwConnectionString)", dwConnectionString)
                .Replace("$(cubeDBName)", cubeDBName)
                .Replace("$(cubeName)", cubeName)
                .Replace("$(DBTableSchemaName)", CONFIGURATION_HELPER.GET_METADATA_PROPERTY("db_table_schema_name"))
                .Replace("$(sid)", sidString);

            sqlHelper.ADD_MESSAGE_LOG(String.Format("[Create base cube] Starting create cube database {0}", cubeDBName), MESSAGE_TYPE.CREATE_CUBE, MESSAGE_RESULT_TYPE.Normal);
            XmlaResultCollection resultCol = cubeServer.Execute(SSASConfiguration);
            foreach (XmlaResult result in resultCol)
            {
                foreach (XmlaMessage message in result.Messages)
                {
                    if (message.ToString().Contains("error") || message.ToString().Contains("failed"))
                    {
                        sqlHelper.ADD_MESSAGE_LOG("[Create base cube]" + message.ToString(), MESSAGE_TYPE.CREATE_CUBE, MESSAGE_RESULT_TYPE.Error);
                        return;
                    }
                    else
                    {
                        sqlHelper.ADD_MESSAGE_LOG("[Create base cube]" + message.ToString(), MESSAGE_TYPE.CREATE_CUBE, MESSAGE_RESULT_TYPE.Succeed);
                    }
                }
            }
            sqlHelper.ADD_MESSAGE_LOG("[Create base cube]" + String.Format("Succeed to create cube database {0}", cubeDBName), MESSAGE_TYPE.CREATE_CUBE, MESSAGE_RESULT_TYPE.Succeed);
        }
        #endregion

        #region CREATE_CUBE_COMMON
        /// <summary>
        /// Create common cube by configured metadata
        /// </summary>
        /// <param name="oledb"></param>
        /// <param name="sqlHelper"></param>
        /// <param name="asMeta"></param>
        /// <param name="cube_server"></param>
        /// <param name="cube_db_name"></param>
        /// <param name="cube_name"></param>
        /// <param name="cubeProcessXMLA"></param>
        /// <param name="is_rolap"></param>
        public void CREATE_CUBE_COMMON(
            DB_SQLHELPER_BASE sqlHelper
            ,IDbConnection oledb
            ,AS_METADATA asMeta
            ,Server cube_server
            ,String cube_db_name
            ,String cube_name
            ,int is_rolap)
        {
            try
            {
                Database cube_db = cube_server.Databases.FindByName(cube_db_name);
                DataSourceView cube_dsv = cube_db.DataSourceViews.FindByName("SSAS_DSV");
                DataSet dsv_schema = cube_dsv.Schema;
                // 1.create dsv
                CREATE_CUBE_DSV(sqlHelper, asMeta, oledb, cube_dsv, dsv_schema.DataSetName);
                cube_dsv.Update();
                // 2.create dimensions
                CREATE_CUBE_DIMENSION(sqlHelper, asMeta, cube_db, cube_dsv);
                cube_db.Update();
                // 3.cube
                Cube cube = cube_db.Cubes.FindByName(cube_name);
                // 4.add dimension into cube
                ADD_DIMENSION_TO_CUBE(sqlHelper, asMeta, cube_db, cube);
                // 5.create measure groups
                CREATE_MEASURE_GROUP(cube_dsv, sqlHelper, asMeta, cube, is_rolap);
                // 6.remove unnecessary objects
                REMOVE_CUBE_OBJECTS(sqlHelper, cube_db, cube);
                // 8.create partitions
                // 9.create aggregations
                // 10.cube customized
                //customized_cube_changes(self,db_connection,cube_server,cube_db,cube,is_rolap);
                sqlHelper.ADD_MESSAGE_LOG(
                    "[Create cube common] Saving cube changes...",
                    MESSAGE_TYPE.CREATE_CUBE,
                    MESSAGE_RESULT_TYPE.Normal);
                //11.cube save
                cube_db.Update(UpdateOptions.ExpandFull);
                sqlHelper.ADD_MESSAGE_LOG(
                    "[Create cube common] Saved cube changes, processing cube...",
                    MESSAGE_TYPE.CREATE_CUBE,
                    MESSAGE_RESULT_TYPE.Normal);
                CUBE_PROCESS_FULL(sqlHelper, cube_server, cube_db_name);
            }
            catch (Exception ex)
            {
                sqlHelper.ADD_MESSAGE_LOG("[Create cube common] " + ex.Message.ToString(), MESSAGE_TYPE.CREATE_CUBE, MESSAGE_RESULT_TYPE.Error);
                throw(ex);
            }
            finally
            {
                //oledb.close_connection();
            }
        }
        #endregion

        #region CREATE_CUBE_DSV
        /// <summary>
        /// Create Create Cube DSV
        /// </summary>
        /// <param name="sqlHelper"></param>
        /// <param name="asMeta"></param>
        /// <param name="oleDB"></param>
        /// <param name="DSV"></param>
        /// <param name="dsvSchema"></param>
        public void CREATE_CUBE_DSV(DB_SQLHELPER_BASE sqlHelper
            , AS_METADATA asMeta
            , IDbConnection oleDB
            , Microsoft.AnalysisServices.DataSourceView DSV
            , String dsvSchema)
        {
            try
            {
                sqlHelper.ADD_MESSAGE_LOG("[Create cube dsv] Starting add cube dsv", MESSAGE_TYPE.DSV, MESSAGE_RESULT_TYPE.Normal);
                DataTable dsvSet = asMeta.GET_SSAS_DSV_SET(sqlHelper);
                foreach (DataRow row in dsvSet.Rows)
                {
                    String dsv_schema_name = row["dsv_schema_name"].ToString();
                    String db_table_name = row["db_table_name"].ToString();
                    String table_type = row["table_type"].ToString();
                    String is_name_query = row["is_named_query"].ToString();
                    String dsv_query_text = String.Format("SELECT * FROM {0} WHERE 1=0", db_table_name);
                    sqlHelper.ADD_MESSAGE_LOG(String.Format("[Create cube dsv->{0}] Adding to add cube dsv", db_table_name), MESSAGE_TYPE.DSV, MESSAGE_RESULT_TYPE.Succeed);
                    AS_API.ADD_TABLE_TO_CUBE_DSV(oleDB
                        , DSV
                        , db_table_name
                        , dsv_query_text
                        , db_table_name
                        ,"View"
                        , CONFIGURATION_HELPER.GET_METADATA_PROPERTY("db_table_schema_name"));
                }
                DSV.Update();
                sqlHelper.ADD_MESSAGE_LOG("[Create cube dsv] Succeed to add cube dsv", MESSAGE_TYPE.DSV, MESSAGE_RESULT_TYPE.Succeed);
            }
            catch (Exception ex)
            {
                sqlHelper.ADD_MESSAGE_LOG("[Create cube dsv] " + ex.Message.ToString(), MESSAGE_TYPE.DSV, MESSAGE_RESULT_TYPE.Error);
                throw(ex);
            }
        }
        #endregion

        #region CREATE_CUBE_DIMENSION
        /// <summary>
        /// Create cube dimension 
        /// </summary>
        /// <param name="sqlHelper"></param>
        /// <param name="asMeta"></param>
        /// <param name="cubeDB"></param>
        /// <param name="DSV"></param>
        public void CREATE_CUBE_DIMENSION(DB_SQLHELPER_BASE sqlHelper
            , AS_METADATA asMeta
            , Microsoft.AnalysisServices.Database cubeDB
            , Microsoft.AnalysisServices.DataSourceView DSV
            )
        {
            try
            {
                sqlHelper.ADD_MESSAGE_LOG("[Create dimension] Starting create dimension"
                            , MESSAGE_TYPE.DIMENSION
                            , MESSAGE_RESULT_TYPE.Normal);
                DataTable DimensionSet = asMeta.GET_SSAS_DIMENSION_SET(sqlHelper);
                foreach (DataRow dimension_row in DimensionSet.Rows)
                {
                    String DimensionID = dimension_row["dimension_id"].ToString();
                    String DimensionName = dimension_row["dimension_name"].ToString();
                    String DimensionType = dimension_row["dimension_type"].ToString();
                    String DataSourceName = dimension_row["dsv_schema_name"].ToString();
                    String dsvName = DSV.Name;
                    Microsoft.AnalysisServices.Dimension dim = AS_API.ADD_DIMENSION(sqlHelper, cubeDB, dsvName, DimensionID, DimensionName, DimensionType);

                    DataTable AttributeSet = asMeta.GET_SSAS_ATTRIBUTES_SET(sqlHelper,DimensionID);
                    if (AttributeSet == null || AttributeSet.Rows == null || AttributeSet.Rows.Count == 0)
                    {
                        sqlHelper.ADD_MESSAGE_LOG(
                            String.Format("[Create dimension] Dimension {0} has not any attributes, is it expected?", DimensionID)
                            , MESSAGE_TYPE.DIMENSION
                            , MESSAGE_RESULT_TYPE.Warning);
                    }
                    else
                    {
                        sqlHelper.ADD_MESSAGE_LOG(
                            String.Format("[Create dimension] Adding {0} attributeds for dimension {1}", AttributeSet.Rows.Count.ToString(), DimensionID)
                            , MESSAGE_TYPE.DIMENSION
                            , MESSAGE_RESULT_TYPE.Normal);
                    }
                    foreach (DataRow attribute_row in AttributeSet.Rows)
                    {
                        String AttributeID = attribute_row["attribute_id"].ToString();
                        String AttributeName = attribute_row["attribbute_name"].ToString();
                        String DSVSchemaName = attribute_row["dsv_schema_name"].ToString();
                        String DBColumn = attribute_row["key_column_db_column"].ToString();
                        String OleDbType = attribute_row["key_column_oledb_type"].ToString();
                        String AttributeUsage = attribute_row["attribute_usage"].ToString();
                        String NameColumn = attribute_row["name_column"].ToString();
                        String Visible = attribute_row["visible"].ToString();
                        String AttHierEnabled = attribute_row["atthier_enabled"].ToString();
                        String OrderBy = attribute_row["order_by"].ToString();
                        Microsoft.AnalysisServices.OrderBy attribute_order_by = Microsoft.AnalysisServices.OrderBy.Name;
                        if (OrderBy.ToLower() == "key")
                        {
                            attribute_order_by = Microsoft.AnalysisServices.OrderBy.Key;
                        }
                        AS_API.ADD_ATTRIBUTE_TO_DIMENSION(
                            sqlHelper,
                            DSV,
                            dim,
                            DataSourceName,
                            DBColumn,
                            AttributeID,
                            AttributeName,
                            AS_API_HELPER.GET_SSAS_OLEDB_TYPE_BY_NAME(OleDbType),
                            AS_API_HELPER.GET_SSAS_ATTRIBUTE_USAGE_BY_NAME(AttributeUsage),
                            NameColumn, Convert.ToBoolean(Visible),
                            Convert.ToBoolean(AttHierEnabled),
                            attribute_order_by
                            );
                    }

                    DataTable AttributeRelationShipSet = asMeta.GET_SSAS_ATTRIBUTE_RELATION_SHIPS_SET(sqlHelper, DimensionID);

                    sqlHelper.ADD_MESSAGE_LOG(String.Format("[Create dimension] Adding {0} attribute relationships for dimension {1}", AttributeRelationShipSet.Rows.Count.ToString(), DimensionID)
                            , MESSAGE_TYPE.ATTRIBUTE_RELATIONSHIP
                            , MESSAGE_RESULT_TYPE.Normal);

                    foreach (DataRow row in AttributeRelationShipSet.Rows)
                    {
                        String BasedAttributeID = row["based_attribute_id"].ToString();
                        String RelatedAttributeID = row["related_attribute_id"].ToString();
                        String RelationShipType = row["relationship_type"].ToString();
                        Microsoft.AnalysisServices.RelationshipType AttributeRelationShipType = AS_API_HELPER.GET_SSAS_ATTRIBUTE_RELATION_SHIP_TYPE_BY_NAME(RelationShipType);

                        AS_API.ADD_ATTRIBUTE_RELATIONSHIP(
                            sqlHelper,
                            dim,
                            BasedAttributeID,
                            RelatedAttributeID,
                            AttributeRelationShipType);
                    }


                    DataTable HierarchiesSet = asMeta.GET_SSAS_HIERARCHIES_SET(sqlHelper, DimensionID);

                    sqlHelper.ADD_MESSAGE_LOG(
                        String.Format("[Create dimension] Adding {0} hierarchy levels for dimension {1}", HierarchiesSet.Rows.Count.ToString(), DimensionID)
                        , MESSAGE_TYPE.HIERARCHIES
                        , MESSAGE_RESULT_TYPE.Normal);
                    foreach (DataRow row in HierarchiesSet.Rows)
                    {
                        String HierarchyName = row["hierarchy_name"].ToString();
                        String LevelName = row["level_name"].ToString();
                        String LevelID = row["level_id"].ToString();
                        String SourceAttributeID = row["source_attribute_id"].ToString();
                        AS_API.ADD_ATTRIBUTE_HIERACHIES(
                            sqlHelper,
                            dim,
                            HierarchyName,
                            LevelName,
                            SourceAttributeID);
                        sqlHelper.ADD_MESSAGE_LOG(
                            "[Create dimension->Hierarchy] |" + new string('_', Convert.ToInt16(LevelID)) + LevelName
                            , MESSAGE_TYPE.HIERARCHIES
                            , MESSAGE_RESULT_TYPE.Normal);
                    }
                    sqlHelper.ADD_MESSAGE_LOG(
                        "[Create dimension] Updating changes of dimension objects.."
                        , MESSAGE_TYPE.DIMENSION
                        , MESSAGE_RESULT_TYPE.Normal);
                    dim.Update();
                    sqlHelper.ADD_MESSAGE_LOG(
                        "[Create dimension] Succeed to add changes ofo dimension objects.."
                        , MESSAGE_TYPE.DIMENSION
                        , MESSAGE_RESULT_TYPE.Normal);
                }
            }
            catch (Exception ex)
            {
                sqlHelper.ADD_MESSAGE_LOG("[Create dimension] " + ex.Message.ToString(), MESSAGE_TYPE.ADD_DIMENSION, MESSAGE_RESULT_TYPE.Error);
                throw (ex);
            }
        }
        #endregion

        #region ADD_DIMENSION_TO_CUBE
        /// <summary>
        /// Add dimension into cube (instead of cube db)
        /// </summary>
        /// <param name="sqlHelper"></param>
        /// <param name="asMeta"></param>
        /// <param name="cubeDB"></param>
        /// <param name="cube"></param>
        public void ADD_DIMENSION_TO_CUBE(DB_SQLHELPER_BASE sqlHelper
            , AS_METADATA asMeta
            , Microsoft.AnalysisServices.Database cubeDB
            , Cube cube
            )
        {
            try
            {
                DataTable DimensionSet = asMeta.GET_SSAS_DIMENSION_SET(sqlHelper);
                foreach (DataRow row in DimensionSet.Rows)
                {
                    String DimensionID = row["DimensionID"].ToString();
                    String DimensionName = row["DimensionName"].ToString();
                    String DimensionType = row["DimensionType"].ToString();
                    AS_API.ADD_CUBE_DIMENSION(
                        sqlHelper,
                        cubeDB,
                        cube,
                        DimensionID,
                        DimensionType);
                    sqlHelper.ADD_MESSAGE_LOG(
                        String.Format("Addedd Dimension {0} into Cube ", DimensionID)
                        , MESSAGE_TYPE.ADD_CUBE_DIMENSION
                        , MESSAGE_RESULT_TYPE.Normal);
                }
                cube.Update(Microsoft.AnalysisServices.UpdateOptions.ExpandFull);
            }
            catch (Exception ex)
            {
                sqlHelper.ADD_MESSAGE_LOG(ex.Message.ToString(), MESSAGE_TYPE.DIMENSION, MESSAGE_RESULT_TYPE.Error);
                throw (ex);
            }
        }
        #endregion

        #region CREATE_MEASURE_GROUP
        /// <summary>
        /// Create measure group 
        /// </summary>
        /// <param name="dsv"></param>
        /// <param name="sqlHelper"></param>
        /// <param name="asMeta"></param>
        /// <param name="cube"></param>
        /// <param name="is_rolap"></param>
        /// <param name="measure_group_id"></param>
        public void CREATE_MEASURE_GROUP(DataSourceView dsv
            , DB_SQLHELPER_BASE sqlHelper
            , AS_METADATA asMeta
            , Cube cube
            , int is_rolap_cube
            , String measure_group_id = null)
        {
            try
            {
                DataTable MGSet = asMeta.GET_SSAS_MEASURE_GROUPS_SET(sqlHelper, is_rolap_cube);
                String DSVSchemaName = "";
                foreach (DataRow row in MGSet.Rows)
                {
                    String measureGroupID = row["measureGroupID"].ToString();
                    String measureGroupName = row["measureGroupName"].ToString();
                    String DependedFactTable = row["DependedFactTable"].ToString();
                    String KeyNotFound_Action = row["KeyNotFound_Action"].ToString();
                    int is_rolap_mg = Convert.ToInt16(row["Is_Rolap_Mg"].ToString());
                    MeasureGroup newMG = AS_API.ADD_MEASURE_GROUP(sqlHelper, cube, measureGroupName, measureGroupID, is_rolap_mg, KeyNotFound_Action);
                    DataTable dimUsageSet = asMeta.GET_SSAS_DIM_USAGE_SET(sqlHelper, measureGroupID);
                    DataTable CoreMeasureSet = asMeta.GET_SSAS_CORE_MEASURES_SET(sqlHelper, measureGroupID);
                    foreach (DataRow measure in CoreMeasureSet.Rows)
                    {
                        measureGroupID = measure["measureGroupID"].ToString();
                        measureGroupName = measure["measureGroupName"].ToString();
                        String MeasureId = measure["MeasureId"].ToString();
                        String MeasureName = measure["MeasureName"].ToString();
                        String MeasureDataType = measure["MeasureDataType"].ToString();
                        String DBColumn = measure["DBColumn"].ToString();
                        DSVSchemaName = measure["DSVSchemaName"].ToString();
                        String AggregationFunction = measure["AggregationFunction"].ToString();
                        String DisplayFolder = measure["DisplayFolder"].ToString();
                        String FormatString = measure["FormatString"].ToString();
                        AS_API.ADD_MEASURE_TO_MEASURE_GROUP(
                            sqlHelper,
                            newMG,
                            DSVSchemaName,
                            DBColumn,
                            MeasureName,
                            MeasureId,
                            DisplayFolder,
                            FormatString,
                            AggregationFunction,
                            true,
                            MeasureDataType,
                            MeasureDataType);
                    }
                    foreach (DataRow dimUsage in dimUsageSet.Rows)
                    {
                        String DimUsageType = dimUsage["dim_usage_type"].ToString();
                        String InternalDimID = dimUsage["internal_dim_id"].ToString();
                        String InternalDimAttrID = dimUsage["internal_dim_attrid"].ToString();
                        DSVSchemaName = dimUsage["dsv_schema_name"].ToString();
                        String factFKDimColumnName = dimUsage["fact_fk_dim_column_name"].ToString();
                        String DataType = dimUsage["fact_fk_dim_column_data_type"].ToString();
                        String DimensionID = dimUsage["dimension_id"].ToString();
                        String AttributeID = dimUsage["attribute_id"].ToString();
                        String InternalMeasureGroupID = dimUsage["internal_measure_group_id"].ToString();
                        switch (DimUsageType.ToLower())
                        {
                            case "regular":
                                DataItem factDataItem = AS_API.CREATE_DATA_ITEM(
                                    sqlHelper,
                                    dsv,
                                    DSVSchemaName,
                                    factFKDimColumnName,
                                    AS_API_HELPER.GET_SSAS_OLEDB_TYPE_BY_NAME(DataType));
                                AS_API.ADD_DIM_USAGE_REGULAR_RELATIONSHIP(
                                    sqlHelper,
                                    cube,
                                    newMG,
                                    factDataItem,
                                    DimensionID,
                                    AttributeID);
                                break;
                            case "reference":
                                AS_API.ADD_DIM_USAGE_REFERENCE_RELATIONSHIP(newMG,
                                                                          DimensionID,
                                                                          AttributeID,
                                                                          InternalDimID,
                                                                          InternalDimAttrID);
                                break;
                            case "manytomany":
                                AS_API.ADD_DIM_USAGE_MANY_RELATIONSHIP(newMG, InternalMeasureGroupID, DimensionID);
                                break;
                            case "fact":
                                AS_API.ADD_DIM_USAGE_FACT_RELATIONSHIP(newMG, InternalDimAttrID, DimensionID);
                                break;
                            default:
                                break;
                        }
                    }
                    AggregationDesign agg_design = CREATE_AGGREGATION_DESIGN(newMG,sqlHelper,asMeta);
                    CREATE_CUBE_PARTITION_FOR_MEASURE_GROUP(sqlHelper, asMeta, cube, is_rolap_cube, newMG, agg_design, is_rolap_mg, DependedFactTable,DSVSchemaName);
                    newMG.Update();
                }
            }
            catch (Exception ex)
            {
                sqlHelper.ADD_MESSAGE_LOG(ex.Message.ToString(), MESSAGE_TYPE.MEASURE_GROUP, MESSAGE_RESULT_TYPE.Error);
                throw (ex);
            }
        }
        #endregion

        #region CREATE_CUBE_PARTITION_FOR_MEASURE_GROUP
        /// <summary>
        /// CREATE_CUBE_PARTITION_FOR_MEASURE_GROUP
        /// </summary>
        /// <param name="sqlHelper"></param>
        /// <param name="asMeta"></param>
        /// <param name="cube"></param>
        /// <param name="is_rolap_cube"></param>
        /// <param name="mg"></param>
        /// <param name="aggregation_design"></param>
        /// <param name="is_rolap_mg"></param>
        /// <param name="depended_fact_table"></param>
        /// <param name="DSVSchemaName"></param>
        public void CREATE_CUBE_PARTITION_FOR_MEASURE_GROUP(
            DB_SQLHELPER_BASE sqlHelper
            , AS_METADATA asMeta
            , Cube cube
            , int is_rolap_cube
            , MeasureGroup mg
            , AggregationDesign aggregation_design
            , int is_rolap_mg
            , String depended_fact_table
            , String DSVSchemaName)
        {
            DataTable partition_date_filter = asMeta.GET_SSAS_PARTITION_SET(sqlHelper, mg.ID);
            String aggregation_design_id = null;
            if (aggregation_design != null)
            {
                aggregation_design_id = aggregation_design.ID.ToString();
            }
            if (partition_date_filter != null && partition_date_filter.Rows != null && partition_date_filter.Rows.Count > 0)
            {
                String factFKDimColumnName = partition_date_filter.Rows[0]["factFKDimColumnName"].ToString();
                int month_volumn_per_partition = Convert.ToInt32(CONFIGURATION_HELPER.GET_METADATA_PROPERTY("month_volumn_per_partition"));
                int year_volumn_per_cube = Convert.ToInt32(CONFIGURATION_HELPER.GET_METADATA_PROPERTY("year_volumn_per_cube"));

                int partitionCount = 0;
                partitionCount = year_volumn_per_cube * 12 / month_volumn_per_partition;

                for (int i = 1; i <= partitionCount + 1; i++)
                {
                    String partitionSelectQuery = "";// String.Format("SELECT * FROM {0} WHERE 1=1 ", DSVSchemaName);

                    //if rolap cube, and current mg is molap, then add where 1=2 filter, select * from tb_name where 1=1 and 1=2
                    if (is_rolap_cube == 1 && is_rolap_mg == 0)
                    {
                        partitionSelectQuery = partitionSelectQuery + " AND 1=2";
                    }
                    //if rolap cube, then no need additional date column filter, if molap , then need date column filter,  
                    //select * from tb_name where 1=1 and dateid>=20100101 and dateid<20100201
                    if (is_rolap_cube == 0)
                    {
                        partitionSelectQuery = partitionSelectQuery + " " + asMeta.GET_SSAS_PARTITION_FILTER(factFKDimColumnName, i, month_volumn_per_partition);
                    }
                    AS_API.CREATE_MOLAP_PARTITION(
                        sqlHelper,
                        mg,
                        "DW_DataSource",
                        mg.ID.ToString() + "_" + i.ToString(),
                        DSVSchemaName,
                        partitionSelectQuery,
                        aggregation_design_id,
                        is_rolap_mg,
                        depended_fact_table
                     );
                    //if rolap cube, then only need one partition
                    if (is_rolap_cube == 1)
                    {
                        break;
                    }
                }
            }
            else
            {
                sqlHelper.ADD_MESSAGE_LOG(String.Format("Create cube partition-> No partition date column been detected for mg {0}", mg.ID), MESSAGE_TYPE.PARTITION, MESSAGE_RESULT_TYPE.Warning);
                AS_API.CREATE_MOLAP_PARTITION(
                    sqlHelper,
                    mg,
                    "DW_DataSource",
                    mg.ID.ToString() + "_1",
                    DSVSchemaName,
                    "",
                    aggregation_design_id,
                    is_rolap_mg,
                    depended_fact_table
                 );
            }
        }
        #endregion

        #region CREATE_AGGREGATION_DESIGN
        /// <summary>
        /// Create aggregation design
        /// </summary>
        /// <param name="mg"></param>
        /// <param name="sqlHelper"></param>
        /// <param name="asMeta"></param>
        /// <returns></returns>
        public AggregationDesign CREATE_AGGREGATION_DESIGN(MeasureGroup mg, DB_SQLHELPER_BASE sqlHelper
            , AS_METADATA asMeta)
        {
            DataTable agg_design_list = null;
            AggregationDesign agg_design = null;
            try
            {
                agg_design_list = asMeta.GET_SSAS_AGGREGATION_DESIGN_SET(sqlHelper, mg.ID);
                foreach (DataRow measure in agg_design_list.Rows)
                {
                    String AggregationDesignName = measure["AggregationDesignName"].ToString();
                    //agg_design=AggregationDesignName;

                    String AggregationName = measure["AggregationName"].ToString();
                    String DimensionID = measure["DimensionID"].ToString();
                    String AttributeID = measure["AttributeID"].ToString();
                    if (mg.AggregationDesigns.Find(AggregationDesignName) == null)
                    {
                        mg.AggregationDesigns.Add(AggregationDesignName);
                    }

                    agg_design = mg.AggregationDesigns[AggregationDesignName];
                    Aggregation agg = agg_design.Aggregations.Find(AggregationName);
                    if (agg == null)
                    {
                        agg = agg_design.Aggregations.Add(AggregationName, AggregationName);
                    }
                    AggregationDimension agg_dim = agg.Dimensions.Find(DimensionID);
                    if (agg_dim == null)
                        agg.Dimensions.Add(DimensionID);
                    agg.Dimensions[DimensionID].Attributes.Add(AttributeID);
                }
            }
            catch (Exception ex)
            {
                sqlHelper.ADD_MESSAGE_LOG(ex.Message.ToString(), MESSAGE_TYPE.AGGREGATION_DESIGN, MESSAGE_RESULT_TYPE.Error);
                throw (ex);
            }
            return agg_design;
        }
        #endregion

        #region CREATE_CUBE_CORE_MEASURES
        /// <summary>
        /// Create cube core measure
        /// </summary>
        /// <param name="sqlHelper"></param>
        /// <param name="asMeta"></param>
        /// <param name="cube"></param>
        public void CREATE_CUBE_CORE_MEASURES(DB_SQLHELPER_BASE sqlHelper
            , AS_METADATA asMeta
            , Cube cube)
        {
            try
            {
                DataTable coreMeasureSet = asMeta.GET_SSAS_CORE_MEASURES_SET(sqlHelper);
                foreach (DataRow row in coreMeasureSet.Rows)
                {
                    String measureGroupID = row["measure_group_id"].ToString();
                    String MeasureId = row["measure_id"].ToString();
                    String MeasureName = row["measure_name"].ToString();
                    String DSVSchemaName = row["dsv_schema_name"].ToString();
                    String DisplayFolder = row["display_folder"].ToString();
                    String FormatString = row["format_string"].ToString();
                    String MeasureDataType = row["measure_data_type"].ToString();
                    String DBColumn = row["db_column"].ToString();
                    String AggregationFunction = row["aggregation_function"].ToString();
                    MeasureGroup measureGroup = cube.MeasureGroups.Find(measureGroupID);
                    AS_API.ADD_MEASURE_TO_MEASURE_GROUP(
                        sqlHelper
                        , measureGroup
                        , DSVSchemaName
                        , DBColumn
                        , MeasureName
                        , MeasureId
                        , DisplayFolder
                        , FormatString
                        , AggregationFunction
                        , true
                        , MeasureDataType
                        , MeasureDataType);
                    measureGroup.Update();
                }
            }
            catch (Exception ex)
            {
                sqlHelper.ADD_MESSAGE_LOG(ex.Message.ToString(), MESSAGE_TYPE.MEASURES, MESSAGE_RESULT_TYPE.Error);
                throw (ex);
            }
        }
        #endregion

        #region REMOVE_CUBE_OBJECTS
        /// <summary>
        /// Remove cube objects (FactBool)
        /// </summary>
        /// <param name="cubedb"></param>
        /// <param name="cube"></param>
        public void REMOVE_CUBE_OBJECTS(DB_SQLHELPER_BASE sqlHelper, Database cubedb, Cube cube)
        {
            AS_API.REMOVE_MEASURE_GROUPS(sqlHelper,cube, "FactBool");
        }
        #endregion

        #region CUBE_PROCESS_FULL
        /// <summary>
        /// Cube full process
        /// </summary>
        /// <param name="cube_server"></param>
        /// <param name="cubeDBName"></param>
        /// <param name="xmla"></param>
        public void CUBE_PROCESS_FULL(DB_SQLHELPER_BASE sqlHelper, Server cube_server,String cubeDBName)
        {
            try
            {
                String cubeProcessXMLAPath = CONFIGURATION_HELPER.BASIC_CONFIGURATION_FOLDER + @"\SSASConfiguration\CubeProcess.xml";
                String cubeProcessXMLA = System.IO.File.ReadAllText(cubeProcessXMLAPath);
                cubeProcessXMLA = cubeProcessXMLA.Replace("$(cubeDBName)", cubeDBName);
                XmlaResultCollection _result = cube_server.Execute(cubeProcessXMLA);
                foreach (XmlaResult _res in _result)
                {
                    foreach (XmlaMessage message in _res.Messages)
                    {
                        sqlHelper.ADD_MESSAGE_LOG(

                            message.ToString(),
                            MESSAGE_TYPE.CUBE_PROCESS,
                            MESSAGE_RESULT_TYPE.Normal);
                    }
                }
                sqlHelper.ADD_MESSAGE_LOG(
                    String.Format("Processed cube {0}", cubeDBName),
                        MESSAGE_TYPE.CUBE_PROCESS,
                        MESSAGE_RESULT_TYPE.Normal);
            }
            catch (Exception ex)
            {
                sqlHelper.ADD_MESSAGE_LOG(ex.Message.ToString(), MESSAGE_TYPE.CUBE_PROCESS, MESSAGE_RESULT_TYPE.Error);
                throw (ex);
            }
        }
        #endregion

        #region UPDATE_CUBE_COMMAND
        /// <summary>
        /// Update Cube Command
        /// </summary>
        /// <param name="cube_server"></param>
        /// <param name="cube_db"></param>
        /// <param name="cube"></param>
        /// <param name="mdx_code"></param>
        public void UPDATE_CUBE_COMMAND(DB_SQLHELPER_BASE sqlHelper, Server cube_server, Database cube_db, Cube cube, String mdx_code)
        {
            MdxScript mdx = new MdxScript();
            if (cube.DefaultMdxScript == null)
            {
                sqlHelper.ADD_MESSAGE_LOG(
 
                    "DefaultMdxScript is none, creating a new one",
                    MESSAGE_TYPE.MDX,
                    MESSAGE_RESULT_TYPE.Normal);

                mdx.ID = cube.MdxScripts.GetNewID();
                mdx.Name = "MDXHelper";
            }
            else
            {
                mdx = cube.DefaultMdxScript;
            }
            Command cmd = new Command();
            if (cube.DefaultMdxScript == null || cube.DefaultMdxScript.Commands == null)
            {
                sqlHelper.ADD_MESSAGE_LOG(

                    "DefaultMdxScript.Commands is none, creating a new one",
                        MESSAGE_TYPE.MDX,
                        MESSAGE_RESULT_TYPE.Normal);
            }
            else
            {
                cmd = cube.DefaultMdxScript.Commands[0];
            }
            cmd.Text = mdx_code;
            mdx.Commands.Remove(cmd);
            mdx.Commands.Add(cmd);
            cube.MdxScripts.Remove(mdx);
            cube.MdxScripts.Add(mdx);
            sqlHelper.ADD_MESSAGE_LOG(
                "Refreshed cube mdx calculations.",
                    MESSAGE_TYPE.MDX,
                    MESSAGE_RESULT_TYPE.Normal);
        }
        #endregion
    }
}
