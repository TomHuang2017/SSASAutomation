using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Xml;

namespace MDXHelper.SSASAutomation
{
    public class DB_SQLHELPER_BASE
    {
        
        String __ConnectionString = "";
        IDbConnection __DBConnection = null;

        String __OleDBConnectionString = "";
        IDbConnection __OleDBConnection = null;
        public String ConnectionString
        {
            get { return this.__ConnectionString; }
            set { this.__ConnectionString = value; }
        }
        public IDbConnection DBConnection
        {
            get { return this.__DBConnection; }
            set { this.__DBConnection = value; }
        }
        public String OleDBConnectionString
        {
            get { return this.__OleDBConnectionString; }
            set { this.__OleDBConnectionString = value; }
        }
        public IDbConnection OleDBConnection
        {
            get { return this.__OleDBConnection; }
            set { this.__OleDBConnection = value; }
        }

        public DB_SQLHELPER_BASE(
            String _ConnectionString
            //String _DBTableSchemaName, 
            //int _SSAS_Partition_Contains_YearVolumn_Data,
            //int _SSAS_MonthVolumn_Per_Partition,
            //int _ConectionTimeOut = 1080
            )
        {
            ConnectionString = _ConnectionString;
        }
        public virtual IDbConnection INIT_CONNECTION()
        {
            return null;
        }

        public virtual IDbConnection GET_OLEDB_CONNECTION(String _oledbConnectionString)
        {
            return null;
        }

        public bool IS_OPEN()
        {
            return (this.DBConnection != null && this.DBConnection.State == ConnectionState.Open);
        }

        public void CLOSE_CONNECTION()
        {
            if (IS_OPEN())
            {
                this.DBConnection.Close();
            }
        }
        //Get data base connection
        public IDbConnection GET_DATABASE_CONNECTION()
        {
            return INIT_CONNECTION();
        }

        //Get database command without connection
        public IDbCommand GET_DATABASE_COMMAND_DEFAULT()
        {
            return null; // new System.Data.SqlClient.SqlCommand();
        }
        public virtual IDbDataAdapter GET_DATABASE_ADAPATER_DEFAULT()
        {
            return null; // new System.Data.SqlClient.SqlDataAdapter();
        }

        //-----------------------------
        //---------Query---------------
        //-----------------------------
        public int EXECUTE_SQL_QUERY(DB_SQLHELPER_BASE sqlHelper,String SqlString)
        {
            int return_value = 0;
            try
            {
                IDbCommand iCom =sqlHelper.GET_DATABASE_CONNECTION().CreateCommand();
                iCom.CommandText = SqlString;
                iCom.CommandTimeout = Convert.ToInt32(CONFIGURATION_HELPER.GET_METADATA_PROPERTY("command_time_out"));
                iCom.ExecuteNonQuery();
                iCom.Dispose();
                return_value = 1;
            }
            catch (Exception ex)
            {
                sqlHelper.ADD_MESSAGE_LOG( ex.Message, MESSAGE_TYPE.SQLQuery, MESSAGE_RESULT_TYPE.Error);
                throw (ex);
            }
            finally
            {
                CLOSE_CONNECTION();
            }
            return return_value;
        }
        public int EXECUTE_PROCEDURE_WITH_PARAMETERS(DB_SQLHELPER_BASE sqlHelper, String StoreProcedureName, IDataParameter[] PassedParameters)
        {
            int return_value = 0;
            try
            {
                IDbCommand iCom = sqlHelper.GET_DATABASE_CONNECTION().CreateCommand();
                iCom.CommandType = System.Data.CommandType.StoredProcedure;
                iCom.CommandText = StoreProcedureName;
                foreach (IDataParameter parameter in PassedParameters)
                {
                    iCom.Parameters.Add(parameter);
                }
                iCom.CommandTimeout = Convert.ToInt32(CONFIGURATION_HELPER.GET_METADATA_PROPERTY("command_time_out")); 
                iCom.ExecuteNonQuery();
                iCom.Dispose();
                return_value = 1;
            }
            catch (Exception ex)
            {
                sqlHelper.ADD_MESSAGE_LOG(ex.Message, MESSAGE_TYPE.SQLQuery, MESSAGE_RESULT_TYPE.Error);
                throw (ex);
            }
            finally
            {
                CLOSE_CONNECTION();
            }
            return return_value;
        }

        public int EXECUTE_PROCEDURE_WITHOUT_PARAMETERS(DB_SQLHELPER_BASE sqlHelper, String StoreProcedureName)
        {
            return EXECUTE_PROCEDURE_WITH_PARAMETERS(sqlHelper,StoreProcedureName, null);
        }

        public DataTable EXECUTE_SQL_QUERY_RETURN_TABLE(DB_SQLHELPER_BASE sqlHelper, String SqlQuery)
        {
            DataTable returnTable = null;
            try
            {
                IDbCommand iCom =sqlHelper.GET_DATABASE_CONNECTION().CreateCommand();
                iCom.CommandText = SqlQuery;
                iCom.CommandTimeout = Convert.ToInt32(CONFIGURATION_HELPER.GET_METADATA_PROPERTY("command_time_out")); ;
                IDbDataAdapter iAdap = GET_DATABASE_ADAPATER_DEFAULT();
                iAdap.SelectCommand = iCom;
                DataSet dataSet = new System.Data.DataSet();
                iAdap.Fill(dataSet);
                iCom.Dispose();
                returnTable = dataSet.Tables[0];
            }
            catch (Exception ex) {
                sqlHelper.ADD_MESSAGE_LOG(ex.Message, MESSAGE_TYPE.SQLQuery, MESSAGE_RESULT_TYPE.Error);
                throw(ex);
            }
            finally
            {
                CLOSE_CONNECTION();
            }
            return returnTable;
        }

        public virtual void ADD_MESSAGE_LOG(
            String _Message_Text
            , MESSAGE_TYPE _Messaage_Type
            , MESSAGE_RESULT_TYPE _Message_Result_Type = MESSAGE_RESULT_TYPE.Succeed)
        {

            //System.Console.WriteLine(String.Format("[{0}][{1}]:[{2}]", _Messaage_Type.ToString(), _Message_Result_Type.ToString(), _Message_Text));
        }
    }
}

