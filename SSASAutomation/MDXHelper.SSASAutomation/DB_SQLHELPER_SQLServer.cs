using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using System.Xml;

namespace MDXHelper.SSASAutomation
{
    public class DB_SQLHELPER_SQLServer: DB_SQLHELPER_BASE
    {
        public DB_SQLHELPER_SQLServer(String _ConnectionString)
            : base(_ConnectionString)
        {}
        public override IDbConnection INIT_CONNECTION()
        {
            if (DBConnection == null)
            {
                this.DBConnection = new System.Data.SqlClient.SqlConnection(this.ConnectionString);
                this.DBConnection.Open();
            }
            else
            {
                if (!IS_OPEN())
                {
                    this.DBConnection.Open();
                }
            }
            return this.DBConnection;
        }
        //Get database command without connection
        public override IDbDataAdapter GET_DATABASE_ADAPATER_DEFAULT()
        {
            return new System.Data.SqlClient.SqlDataAdapter();
        }

        public override String GET_OLEDB_CONNECTION_STRING(String _oledbConnectionString)
        {
            String returnOledbConnectionString = _oledbConnectionString;
            if (_oledbConnectionString.IndexOf("Provider") <= 1)
            {
                returnOledbConnectionString = "Provider=SQLNCLI11.1;" + _oledbConnectionString;
            }
            else
            {
                returnOledbConnectionString = _oledbConnectionString;
            }
            return returnOledbConnectionString;
        }
        public override IDbConnection GET_OLEDB_CONNECTION(String _oledbConnectionString)
        {
            System.Data.OleDb.OleDbConnection oledb=new System.Data.OleDb.OleDbConnection();
            oledb.ConnectionString = GET_OLEDB_CONNECTION_STRING(_oledbConnectionString);
            oledb.Open();
            return  oledb;
        }
        public override void ADD_MESSAGE_LOG(
            String _Message_Text
            , MESSAGE_TYPE _Messaage_Type
            , MESSAGE_RESULT_TYPE _Message_Result_Type = MESSAGE_RESULT_TYPE.Succeed)
        {
            List<SqlParameter> sqlParaList = new List<SqlParameter>();
            SqlParameter message_type = new SqlParameter("@message_type", SqlDbType.NVarChar);
            message_type.Value = _Messaage_Type.ToString();
            sqlParaList.Add(message_type);
            SqlParameter message_result = new SqlParameter("@message_result", SqlDbType.NVarChar);
            message_result.Value = _Message_Result_Type.ToString();
            sqlParaList.Add(message_result);
            SqlParameter message_description = new SqlParameter("@message_description", SqlDbType.NVarChar);
            message_description.Value = _Message_Text;
            sqlParaList.Add(message_description);

            SqlParameter[] parameters = sqlParaList.ToArray();
            EXECUTE_PROCEDURE_WITH_PARAMETERS(this, "sp_ssas_automation_deploy_log_details", parameters);
        }
    }
}
