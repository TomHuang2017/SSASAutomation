using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AnalysisServices;
using System.Data;
using MDXParser;

namespace MDXHelper.SSASAutomation
{
    public static class AS_API_HELPER
    {
        #region Create SSAS Cube Server Connection
        /// <summary>
        /// Create SSAS Cube Server Connection
        /// </summary>
        /// <param name="cubeServerName"></param>
        /// <returns></returns>
        public static Microsoft.AnalysisServices.Server CREATE_OLAP_CONNECTION(String cubeServerName,String cubeName=null)
        {
            String ConnecteString = String.Format("Data Source ={0}; Provider=msolap",cubeServerName);
            if (cubeName != null)
            {
                ConnecteString = String.Format("Data Source ={0};;Initial Catalog={1}; Connect Timeout=600;Provider=msolap", cubeServerName, cubeName);
            }
            Microsoft.AnalysisServices.Server asServer=new Microsoft.AnalysisServices.Server();
            asServer.Connect(ConnecteString);
            return asServer;
        }
        #endregion

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

        #region Check MDX Syntax is validate
        /// <summary>
        /// Check MDX Syntax is validate
        /// </summary>
        /// <param name="mdxText"></param>
        /// <returns></returns>
        public static bool checkMDXSyntax(String mdxText)
        {
            bool return_value = false;
            int IsParseSuccessfully = 0;
            MDXParser.MDXParser parser = Parse(null, false, false, false, mdxText, out IsParseSuccessfully);
            if (parser != null && IsParseSuccessfully == 1)
            {
                return_value = true;
            }
            return return_value;
        }
        #endregion

        #region Parse MDX Syntax
        /// <summary>
        /// Parse MDX Syntax
        /// </summary>
        /// <param name="text"></param>
        /// <param name="FillTree"></param>
        /// <param name="IsExpression"></param>
        /// <param name="TryAnother"></param>
        /// <param name="mdx"></param>
        /// <param name="IsParseSuccessfully"></param>
        /// <returns></returns>
        public static MDXParser.MDXParser Parse(string text, bool FillTree, bool IsExpression, bool TryAnother, String mdx, out int IsParseSuccessfully)
        {
            IsParseSuccessfully = 1;
            Locator l = new Locator();

            MDXParser.CubeInfo cb = null;
            if (cb == null)
            {
                cb = new MDXParser.CubeInfo();
            }
            //TextBoxSource src = new TextBoxSource(currentMdxEditor, l);
            Source src = new Source();
            MDXParser.MDXParser parser = new MDXParser.MDXParser(mdx, src, cb);

            try
            {
                if (IsExpression)
                {
                    parser.ParseExpression();
                }
                else
                {
                    parser.Parse(false);
                }
            }
            catch (MDXParserException exception)
            {
                IsParseSuccessfully = 0;
                if (TryAnother)
                {
                    try
                    {
                        IsParseSuccessfully = 1;
                        if (IsExpression)
                        {
                            parser.Parse();
                        }
                        else
                        {
                            parser.ParseExpression();
                        }
                        goto Label_016E;
                    }
                    catch (Exception)
                    {
                        //this.m_MessagesGrid.Populate(exception.Messages);
                        IsParseSuccessfully = 0;
                        return parser;
                    }
                }
            }
            finally
            {
            }
        Label_016E:
            if (FillTree)
            {
                MDXNode node = parser.GetNode();
            }
            return parser;
        }
        #endregion
    }
}
