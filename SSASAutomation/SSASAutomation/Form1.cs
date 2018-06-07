using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using MDXHelper.SSASAutomation;
using Microsoft.AnalysisServices;

namespace SSASAutomation
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
        }

        private void Bt_BuildCube_Click(object sender, EventArgs e)
        {

            try
            {
                CONFIGURATION_HELPER.BASIC_CONFIGURATION_FOLDER = @"E:\2.自己文档\BigData\SSASAutomation\SSASAutomation\MDXHelper.SSASAutomation";
                String sqlConnectionString = @"Data Source=.\sql2012;Initial Catalog=CLB_Hospital_DW;Integrated Security=SSPI";
                DB_SQLHELPER_SQLServer sqlHelper = new DB_SQLHELPER_SQLServer(sqlConnectionString);
                AS_METADATA_SQLServer asMeta = new AS_METADATA_SQLServer();

                IDbConnection db_connection = sqlHelper.GET_DATABASE_CONNECTION();

                String oleDBConnectionString = sqlConnectionString;
                IDbConnection oledb_connection = sqlHelper.GET_OLEDB_CONNECTION(oleDBConnectionString);
                String cube_server_name = @".\sql2012";
                String cubeDBName = "SSASCubeDB";
                String cubeName = "SSASCube";
                String DWConnectionString = @"Provider=SQLNCLI11.1;Data Source=.\sql2012;Integrated Security=SSPI;Initial Catalog=CLB_Hospital_DW";
                Server cubeServer = AS_API_HELPER.CREATE_OLAP_CONNECTION(cube_server_name, null);
                AS_API_CUBE_CREATOR cubeCreator = new AS_API_CUBE_CREATOR();
                cubeCreator.CREATE_SSAS_BASE_CUBE(sqlHelper, cubeServer, cubeDBName, cubeName, DWConnectionString);
                cubeServer.Disconnect();

                cubeServer = AS_API_HELPER.CREATE_OLAP_CONNECTION(cube_server_name, null);
                IDbConnection a = new System.Data.OleDb.OleDbConnection();
                cubeCreator.CREATE_CUBE_COMMON(sqlHelper, oledb_connection, asMeta, cubeServer, cubeDBName, cubeName, 0);
                cubeServer.Disconnect();
            }
            catch (Exception ex)
            {
                throw (ex);
            }
        }

        private void Form1_Load(object sender, EventArgs e)
        {
            this.Bt_BuildCube_Click(this.Bt_BuildCube, e);
        }
    }
}

