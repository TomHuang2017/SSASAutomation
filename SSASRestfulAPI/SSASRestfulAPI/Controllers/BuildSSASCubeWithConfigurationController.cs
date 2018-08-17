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
    public class BuildSSASCubeWithConfigurationController : ApiController
    {
        MessageStatus[] buildCube = new MessageStatus[] 
        { 
            new MessageStatus { Message = "succeed to build ssas cube.",Status=1}
        };

        //POST:  /api/BuildSSASCubeWithConfiguration
        public IEnumerable<MessageStatus> BuildSSASCubeWithConfiguration([FromBody] BuildSSASCubeWithConfiguration Pameters)
        {
            try
            {
                String SSASConfigurationPath = AppDomain.CurrentDomain.BaseDirectory + @"App_Data";
                CONFIGURATION_HELPER.BASIC_CONFIGURATION_FOLDER = SSASConfigurationPath;// @"E:\2.自己文档\BigData\SSASAutomation\SSASAutomation\MDXHelper.SSASAutomation";
                
                String clrOlapServer = Pameters.clrOlapServer;
                String clrOlapCubeDB = Pameters.clrOlapCubeDB;
                String clrOlapCube = Pameters.clrOlapCube;
                String clrDWServer = Pameters.clrOlapServer;
                String clrDWName = Pameters.clrDWName;
                String clrDWConnectionString = Pameters.clrDWConnectionString;
                String clrSSASConfigurationConnectionString = Pameters.clrSSASConfigurationConnectionString;

                // @"Data Source=.\SQL2012;Initial Catalog=CLB_Hospital_DW;Integrated Security=SSPI";
                DB_SQLHELPER_SQLServer sqlHelper = new DB_SQLHELPER_SQLServer(clrSSASConfigurationConnectionString);
                AS_METADATA_SQLServer asMeta = new AS_METADATA_SQLServer();
                IDbConnection db_connection = sqlHelper.GET_DATABASE_CONNECTION();
                String oleDBConnectionString = clrSSASConfigurationConnectionString;
                IDbConnection oledb_connection = sqlHelper.GET_OLEDB_CONNECTION(clrDWConnectionString);
                String cube_server_name = clrOlapServer;
                String cubeDBName = clrOlapCubeDB;
                String cubeName = clrOlapCube;
                String dwServer = clrDWServer;
                String dwName = clrDWName;
                // DW链接字符串和SSASAutomation的数据库连接字符串都提供如下格式的链接字符串
                //@"Provider=SQLNCLI11.1;Data Source=" + dwServer + ";Integrated Security=SSPI;Initial Catalog=" + clrDWName;
                //@"Data Source=.\SQL2012;Integrated Security=SSPI;Initial Catalog=CLB_Hospital_DW";
                //@"Data Source=.\SQL2012;Password=**********;User ID=sa;Initial Catalog=CLB_Hospital_DW"

                //Oledb会在DWConnectionString前面加上Provider=SQLNCLI11.1;
                //Oledb会在SSAS的DataSource里设置，以及在DataSourceView里使用
                Server cubeServer = AS_API_HELPER.CREATE_OLAP_CONNECTION(cube_server_name, null);
                AS_API_CUBE_CREATOR cubeCreator = new AS_API_CUBE_CREATOR();
                cubeCreator.CREATE_SSAS_BASE_CUBE(sqlHelper, cubeServer, cubeDBName, cubeName, sqlHelper.GET_OLEDB_CONNECTION_STRING(clrDWConnectionString));
                cubeServer.Disconnect();

                cubeServer = AS_API_HELPER.CREATE_OLAP_CONNECTION(cube_server_name, null);
                IDbConnection a = new System.Data.OleDb.OleDbConnection();
                cubeCreator.CREATE_CUBE_COMMON(sqlHelper, oledb_connection, asMeta, cubeServer, cubeDBName, cubeName, 0);
                cubeServer.Disconnect();
            }
            catch (Exception ex)
            {
                buildCube = new MessageStatus[] 
                { 
                    new MessageStatus { Message ="failed to build ssas cube, message:"+ex.Message.ToString(),Status=0 }
                };
            }
            return buildCube;
        }
    }
}
