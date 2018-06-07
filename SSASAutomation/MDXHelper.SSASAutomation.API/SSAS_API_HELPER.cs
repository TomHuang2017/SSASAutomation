using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AnalysisServices;
using System.Data;


namespace MDXHelper.SSASAutomation.API
{
    public static class SSAS_API_HELPER
    {
        #region Get SSAS measure dataType by name
        /// <summary>
        /// Get SSAS measure dataType by name
        /// </summary>
        /// <param name="name">measure dataType string format</param>
        /// <returns></returns>
        public static Microsoft.AnalysisServices.MeasureDataType GET_SSAS_MEASURE_DATA_TYPE_BY_NAME(String name)
        {
            MeasureDataType returnValue = MeasureDataType.Double;
            String dataType = name.ToLower();
            switch (dataType)
            {
                case "integer":
                    returnValue = MeasureDataType.Integer;
                    break;
                case "bigint":
                    returnValue = MeasureDataType.BigInt;
                    break;
                case "wchar":
                    returnValue = MeasureDataType.WChar;
                    break;
                case "double":
                    returnValue = MeasureDataType.Double;
                    break;
                default:
                    break;
            }
            //helper.print_message_to_client("The measure data type of ["+dataType+"] didn't be defined yet!",'warning')
            return returnValue;
        }
        #endregion

        #region Get SSAS oleDb type by name
        /// <summary>
        /// Get SSAS oleDb type by name
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public static System.Data.OleDb.OleDbType GET_SSAS_OLEDB_TYPE_BY_NAME(String name)
        {
            System.Data.OleDb.OleDbType returnValue = System.Data.OleDb.OleDbType.Integer;
            switch (name.ToLower())
            {
                case "integer":
                    returnValue = System.Data.OleDb.OleDbType.Integer;
                    break;
                case "double":
                    returnValue = System.Data.OleDb.OleDbType.Double;
                    break;
                case "bigint":
                    returnValue = System.Data.OleDb.OleDbType.BigInt;
                    break;
                case "wchar":
                    returnValue = System.Data.OleDb.OleDbType.WChar;
                    break;
                case "smallint":
                    returnValue = System.Data.OleDb.OleDbType.SmallInt;
                    break;
                case "tinyint":
                    returnValue = System.Data.OleDb.OleDbType.TinyInt;
                    break;
                case "numeric":
                    returnValue = System.Data.OleDb.OleDbType.Numeric;
                    break;
                case "date":
                    returnValue = System.Data.OleDb.OleDbType.Date;
                    break;
                case "boolean":
                    returnValue = System.Data.OleDb.OleDbType.Boolean;
                    break;
                case "char":
                    returnValue = System.Data.OleDb.OleDbType.Char;
                    break;
                default:
                    break;
            }
            return returnValue;
        }
        #endregion

        #region Get SSAS attribute usage by name

        /// <summary>
        /// Get SSAS attribute usage by name
        /// </summary>
        /// <param name="usageName"></param>
        /// <returns></returns>
        public static Microsoft.AnalysisServices.AttributeUsage GET_SSAS_ATTRIBUTE_USAGE_BY_NAME(String usageName)
        {
            Microsoft.AnalysisServices.AttributeUsage returnType = Microsoft.AnalysisServices.AttributeUsage.Regular;
            switch (usageName.ToLower())
            {
                case "key":
                    returnType = Microsoft.AnalysisServices.AttributeUsage.Key;
                    break;
                case "parent":
                    returnType = Microsoft.AnalysisServices.AttributeUsage.Parent;
                    break;
                case "regular":
                    returnType = Microsoft.AnalysisServices.AttributeUsage.Regular;
                    break;
                default:
                    //helper.print_message_to_client("You are typing an invalid attribute usage "+usageName,'warning')
                    break;
            }
            return returnType;
        }
        #endregion

        #region Get SSAS attribute relation ship type by name
        /// <summary>
        /// Get SSAS attribute relation ship type by name
        /// </summary>
        /// <param name="typename"></param>
        /// <returns></returns>
        public static Microsoft.AnalysisServices.RelationshipType GET_SSAS_ATTRIBUTE_RELATION_SHIP_TYPE_BY_NAME(String typename)
        {
            Microsoft.AnalysisServices.RelationshipType returnType = Microsoft.AnalysisServices.RelationshipType.Flexible;
            switch (typename.ToLower())
            {
                case "flexible":
                    returnType = Microsoft.AnalysisServices.RelationshipType.Flexible;
                    break;
                case "rigid":
                    returnType = Microsoft.AnalysisServices.RelationshipType.Rigid;
                    break;
                default:
                    //helper.print_message_to_client("You are typing an invalid attribute relationShipType "+typename,'warning')
                    break;
            }
            return returnType;
        }
        #endregion

        #region Get SSAS aggregation function by name
        /// <summary>
        /// Get SSAS aggregation function by name
        /// </summary>
        /// <param name="aggregationFunction"></param>
        /// <returns></returns>
        public static Microsoft.AnalysisServices.AggregationFunction GET_SSAS_AGGREGATION_FUNCTION_BY_NAME(String aggregationFunction)
        {
            AggregationFunction return_agg_fun = AggregationFunction.Sum;
            switch (aggregationFunction.ToLower())
            {
                case "lastnonempty":
                    return_agg_fun = Microsoft.AnalysisServices.AggregationFunction.LastNonEmpty;
                    break;
                case "averageofchildren":
                    return_agg_fun = Microsoft.AnalysisServices.AggregationFunction.AverageOfChildren;
                    break;
                case "count":
                    return_agg_fun = Microsoft.AnalysisServices.AggregationFunction.Count;
                    break;
                case "max":
                    return_agg_fun = Microsoft.AnalysisServices.AggregationFunction.Max;
                    break;
                case "min":
                    return_agg_fun = Microsoft.AnalysisServices.AggregationFunction.Min;
                    break;
                case "none":
                    return_agg_fun = Microsoft.AnalysisServices.AggregationFunction.None;
                    break;
                case "lastchild":
                    return_agg_fun = Microsoft.AnalysisServices.AggregationFunction.LastChild;
                    break;
                case "distinctcount":
                    return_agg_fun = Microsoft.AnalysisServices.AggregationFunction.DistinctCount;
                    break;
                case "byaccount":
                    return_agg_fun = Microsoft.AnalysisServices.AggregationFunction.ByAccount;
                    break;
                case "firstnonempty":
                    return_agg_fun = Microsoft.AnalysisServices.AggregationFunction.FirstNonEmpty;
                    break;
                case "sum":
                    return_agg_fun = Microsoft.AnalysisServices.AggregationFunction.Sum;
                    break;
                case "firstchild":
                    return_agg_fun = Microsoft.AnalysisServices.AggregationFunction.FirstChild;
                    break;
                default:
                    break;
            }
            return return_agg_fun;
        }
        #endregion
    }
}
