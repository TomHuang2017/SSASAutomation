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
        BuildSSASCube[] buildCube = new BuildSSASCube[] 
        { 
            new BuildSSASCube { BuildSSASCubeMessage = "succeed to build ssas cube.",Status=1}
        };

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
            //this.Bt_BuildCube_Click(this.Bt_BuildCube, e);
            //TestCube();
            CONFIGURATION_HELPER.BASIC_CONFIGURATION_FOLDER = @"E:\2.自己文档\BigData\SSASAutomation\SSASAutomation\MDXHelper.SSASAutomation";
                
            //UpdateCubeMDX();
            BuildSSASCubeWithParameters();
        }

        public void TestCube()
        { 
            
            String mdx="Calculate; Create Member CurrentCube.[Measures].[Total Sales] [Measures].[Dollars] * [Measures].[Units];";

            bool i = false;
            i = AS_API_HELPER.checkMDXSyntax(mdx);
            if (i)
            {
                MessageBox.Show("Succeed");
            }
        }
        public IEnumerable<BuildSSASCube> BuildSSASCube()
        {
            try
            {
                String SSASConfigurationPath = AppDomain.CurrentDomain.BaseDirectory + @"App_Data";
                CONFIGURATION_HELPER.BASIC_CONFIGURATION_FOLDER = SSASConfigurationPath;// @"E:\2.自己文档\BigData\SSASAutomation\SSASAutomation\MDXHelper.SSASAutomation";

                String sqlConnectionString = CONFIGURATION_HELPER.GET_METADATA_PROPERTY("dw_connection_string");// @"Data Source=.\SQL2012;Initial Catalog=CLB_Hospital_DW;Integrated Security=SSPI";
                String cube_server_name = CONFIGURATION_HELPER.GET_METADATA_PROPERTY("clr.olap.server");// @".\SQL2012";
                String cubeDBName = CONFIGURATION_HELPER.GET_METADATA_PROPERTY("clr.olap.cubedb");
                String cubeName = CONFIGURATION_HELPER.GET_METADATA_PROPERTY("clr.olap.cube");
                String dwServer = CONFIGURATION_HELPER.GET_METADATA_PROPERTY("clr.db.server");
                String dwDB = CONFIGURATION_HELPER.GET_METADATA_PROPERTY("clr.db.dw");


                DB_SQLHELPER_SQLServer sqlHelper = new DB_SQLHELPER_SQLServer(sqlConnectionString);
                AS_METADATA_SQLServer asMeta = new AS_METADATA_SQLServer();

                IDbConnection db_connection = sqlHelper.GET_DATABASE_CONNECTION();

                String oleDBConnectionString = sqlConnectionString;
                IDbConnection oledb_connection = sqlHelper.GET_OLEDB_CONNECTION(oleDBConnectionString);

                String DWConnectionString = @"Provider=SQLNCLI11.1;Data Source=" + dwServer + ";Integrated Security=SSPI;Initial Catalog=" + dwDB;
                //@"Provider=SQLNCLI11.1;Data Source=.\SQL2012;Integrated Security=SSPI;Initial Catalog=CLB_Hospital_DW";
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
                buildCube = new BuildSSASCube[] 
                            { 
                                new BuildSSASCube { BuildSSASCubeMessage = "failed to build ssas cube, message:"+ex.Message.ToString(),Status=0 }
                            };
            }
            return buildCube;
        }


        #region BuildSSASCubeWithParameters
        public IEnumerable<BuildSSASCube> BuildSSASCubeWithParameters(
            String clrOlapServer = "",
            String clrOlapCubeDB = "",
            String clrOlapCube= "",
            String clrDbServer = "",
            String clrDWName = "",
            String clrDWConnectionString=""
            )
        {
            try
            {
                clrDWConnectionString =  @"Data Source=.\SQL2012;Initial Catalog=CLB_Hospital_DW;Integrated Security=SSPI";
                clrOlapServer = @".\SQL2012";
                clrOlapCubeDB = "SSASCubeDB";
                clrOlapCube = "SSASCube";

                //clrDWConnectionString = CONFIGURATION_HELPER.GET_METADATA_PROPERTY("dw_connection_string");// @"Data Source=.\SQL2012;Initial Catalog=CLB_Hospital_DW;Integrated Security=SSPI";
                //clrOlapServer = CONFIGURATION_HELPER.GET_METADATA_PROPERTY("clr.olap.server");// @".\SQL2012";
                //clrOlapCubeDB = CONFIGURATION_HELPER.GET_METADATA_PROPERTY("clr.olap.cubedb");
                //clrOlapCube = CONFIGURATION_HELPER.GET_METADATA_PROPERTY("clr.olap.cube");
                clrDbServer = @".\SQL2012";
                clrDWName = "CLB_Hospital_DW";

                String sqlConnectionString = clrDWConnectionString;
                // @"Data Source=.\SQL2012;Initial Catalog=CLB_Hospital_DW;Integrated Security=SSPI";
                DB_SQLHELPER_SQLServer sqlHelper = new DB_SQLHELPER_SQLServer(sqlConnectionString);
                AS_METADATA_SQLServer asMeta = new AS_METADATA_SQLServer();

                IDbConnection db_connection = sqlHelper.GET_DATABASE_CONNECTION();

                String oleDBConnectionString = sqlConnectionString;
                IDbConnection oledb_connection = sqlHelper.GET_OLEDB_CONNECTION(oleDBConnectionString);
                String cube_server_name = clrOlapServer;// @".\SQL2012";
                String cubeDBName = clrOlapCubeDB;
                String cubeName = clrOlapCube;
                String dwServer = clrDbServer;
                String dwName = clrDWName;
                String DWConnectionString = @"Provider=SQLNCLI11.1;Data Source=" + dwServer + ";Integrated Security=SSPI;Initial Catalog=" + clrDWName;
                //@"Provider=SQLNCLI11.1;Data Source=.\SQL2012;Integrated Security=SSPI;Initial Catalog=CLB_Hospital_DW";
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
                buildCube = new BuildSSASCube[] 
                            { 
                                new BuildSSASCube { BuildSSASCubeMessage = "failed to build ssas cube, message:"+ex.Message.ToString(),Status=0 }
                            };
            }
            return buildCube;
        }
        #endregion

        #region UpdateCubeMDX
        /// <summary>
        /// 
        /// </summary>
        /// <param name="mdxCaculationScript"></param>
        /// <param name="clrOlapServer"></param>
        /// <param name="clrOlapCubeDB"></param>
        /// <param name="clrOlapCube"></param>
        /// <param name="clrDWConnectionString"></param>
        /// <returns></returns>
        public IEnumerable<BuildSSASCube> UpdateCubeMDX(
            String mdxCaculationScript="",
            String clrOlapServer = "",
            String clrOlapCubeDB = "",
            String clrOlapCube = "",
            String clrDWConnectionString= "")
        {
            try
            {
                clrDWConnectionString = CONFIGURATION_HELPER.GET_METADATA_PROPERTY("dw_connection_string");// @"Data Source=.\SQL2012;Initial Catalog=CLB_Hospital_DW;Integrated Security=SSPI";
                clrOlapServer = @".\SQL2012";
                clrOlapCubeDB = "SSASCubeDB";
                clrOlapCube = "SSASCube";
                mdxCaculationScript = @"
Calculate; 
Create Member CurrentCube.[Measures].[Total Sales] as 
[Measures].[门诊人次]  [Measures].[门急诊人次];";

                IEnumerator<BuildSSASCube> mdxIE=CheckMDXSyntax(mdxCaculationScript).GetEnumerator();
                
                while (mdxIE.MoveNext())
                {
                    if (mdxIE.Current.Status == 1)
                    {
                        String sqlConnectionString =  @"Data Source=.\SQL2012;Initial Catalog=CLB_Hospital_DW;Integrated Security=SSPI";
                        DB_SQLHELPER_SQLServer sqlHelper = new DB_SQLHELPER_SQLServer(sqlConnectionString);

                        String cube_server_name = clrOlapServer;// @".\SQL2012";
                        String cubeDBName = clrOlapCubeDB;
                        String cubeName = clrOlapCube;

                        Server cubeServer = AS_API_HELPER.CREATE_OLAP_CONNECTION(cube_server_name, null);
                        AS_API_CUBE_CREATOR cubeCreator = new AS_API_CUBE_CREATOR();
                        Database CubeDB = cubeServer.Databases.FindByName(cubeDBName);
                        Cube CubeObg = CubeDB.Cubes.FindByName(cubeName);
                        cubeCreator.UPDATE_CUBE_COMMAND(sqlHelper, cubeServer, CubeDB, CubeObg, mdxCaculationScript);
                        CubeDB.Update(Microsoft.AnalysisServices.UpdateOptions.ExpandFull);
                    }
                    break;
                }
            }
            catch (Exception ex)
            {
                buildCube = new BuildSSASCube[] 
                            { 
                                new BuildSSASCube { BuildSSASCubeMessage = "failed to update cube mdx, message:"+ex.Message.ToString(),Status=0 }
                            };
            }
            return buildCube;
        }
        #endregion

        #region CheckMDXSyntax

        public IEnumerable<BuildSSASCube> CheckMDXSyntax(String mdxCaculationScript)
        {
            try
            {
                bool isValid = false;
                isValid = AS_API_HELPER.checkMDXSyntax(mdxCaculationScript);
                if (isValid)
                {
                    buildCube = new BuildSSASCube[] 
                            { 
                                new BuildSSASCube { BuildSSASCubeMessage = "MDX syntax is valid",Status=1}
                            };
                }
                else
                {
                    buildCube = new BuildSSASCube[] 
                            { 
                                new BuildSSASCube { BuildSSASCubeMessage = "MDX syntax is invalid",Status=0}
                            };
                }

            }
            catch (Exception ex)
            {
                buildCube = new BuildSSASCube[] 
                            { 
                                new BuildSSASCube { BuildSSASCubeMessage = "failed to check mdx syntax, message:"+ex.Message.ToString(),Status=0 }
                            };
            }
            return buildCube;
        }
        #endregion
    }
}

