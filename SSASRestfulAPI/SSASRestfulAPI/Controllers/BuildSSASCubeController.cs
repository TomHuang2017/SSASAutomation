using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using SSASRestfulAPI.Models;
using MDXHelper.SSASAutomation;
using System.Data;
using Microsoft.AnalysisServices;

namespace SSASRestfulAPI.Controllers
{
    public class BuildSSASCubeController : ApiController
    {
        BuildSSASCube[] buildCube = new BuildSSASCube[] 
        { 
            new BuildSSASCube { BuildSSASCubeMessage = "succeed to build ssas cube.",Status=1}
        };
        //GET:  /api/BuildSSASCube
        public IEnumerable<BuildSSASCube> GetBuildSSASCube()
        {
            try
            {
                String SSASConfigurationPath = AppDomain.CurrentDomain.BaseDirectory + @"App_Data";

                CONFIGURATION_HELPER.BASIC_CONFIGURATION_FOLDER = SSASConfigurationPath;// @"E:\2.自己文档\BigData\SSASAutomation\SSASAutomation\MDXHelper.SSASAutomation";
                String sqlConnectionString = CONFIGURATION_HELPER.GET_METADATA_PROPERTY("dw_connection_string");// @"Data Source=.\SQL2012;Initial Catalog=CLB_Hospital_DW;Integrated Security=SSPI";
                DB_SQLHELPER_SQLServer sqlHelper = new DB_SQLHELPER_SQLServer(sqlConnectionString);
                AS_METADATA_SQLServer asMeta = new AS_METADATA_SQLServer();

                IDbConnection db_connection = sqlHelper.GET_DATABASE_CONNECTION();

                String oleDBConnectionString = sqlConnectionString;
                IDbConnection oledb_connection = sqlHelper.GET_OLEDB_CONNECTION(oleDBConnectionString);
                String cube_server_name = CONFIGURATION_HELPER.GET_METADATA_PROPERTY("clr.olap.server");// @".\SQL2012";
                String cubeDBName = CONFIGURATION_HELPER.GET_METADATA_PROPERTY("clr.olap.cubedb");
                String cubeName = CONFIGURATION_HELPER.GET_METADATA_PROPERTY("clr.olap.cube");
                String dwServer = CONFIGURATION_HELPER.GET_METADATA_PROPERTY("clr.db.server");
                String DWConnectionString = @"Provider=SQLNCLI11.1;Data Source=" + dwServer + ";Integrated Security=SSPI;Initial Catalog=" + CONFIGURATION_HELPER.GET_METADATA_PROPERTY("clr.db.dw") ;
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
            catch(Exception ex)
            {
                buildCube = new BuildSSASCube[] 
                            { 
                                new BuildSSASCube { BuildSSASCubeMessage = "failed to build ssas cube, message:"+ex.Message.ToString(),Status=0 }
                            };
            }
            return buildCube;
        }
    }
}
