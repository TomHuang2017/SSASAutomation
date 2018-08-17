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
    public class UpdateCubeMDXWithConfigurationController : ApiController
    {
        MessageStatus[] buildCube = new MessageStatus[] 
        { 
            new MessageStatus { Message = "succeed to update mdx.",Status=1}
        };


        //POST:  /api/UpdateCubeMDXWithConfiguration
        public IEnumerable<MessageStatus> UpdateCubeMDXWithConfiguration([FromBody]UpdateMDXCalculation Parameters)
        {
            try
            {
                String mdxCaculationScript = Parameters.mdxCaculationScript;
                String clrOlapServer = Parameters.clrOlapServer;
                String clrOlapCubeDB = Parameters.clrOlapCubeDB;
                String clrOlapCube = Parameters.clrOlapCube;
                String clrSSASConfigurationConnectionString = Parameters.clrSSASConfigurationConnectionString;

                IEnumerator<MessageStatus> mdxIE = (new CheckMDXSyntaxController()).CheckMDXSyntax(new CheckMDXSyntax(mdxCaculationScript)).GetEnumerator();

                while (mdxIE.MoveNext())
                {
                    if (mdxIE.Current.Status == 1)
                    {
                        // @"Data Source=.\SQL2012;Initial Catalog=CLB_Hospital_DW;Integrated Security=SSPI";
                        DB_SQLHELPER_SQLServer sqlHelper = new DB_SQLHELPER_SQLServer(clrSSASConfigurationConnectionString);
                        String SSASConfigurationPath = AppDomain.CurrentDomain.BaseDirectory + @"App_Data";
                        CONFIGURATION_HELPER.BASIC_CONFIGURATION_FOLDER = SSASConfigurationPath;// @"E:\2.自己文档\BigData\SSASAutomation\SSASAutomation\MDXHelper.SSASAutomation";
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
                    else
                    {
                        buildCube = new MessageStatus[] 
                        { 
                            new MessageStatus { Message = "MDX Syntax is invalid.",Status=0}
                        };
                    }
                    break;
                }
            }
            catch (Exception ex)
            {
                buildCube = new MessageStatus[] 
                { 
                    new MessageStatus { Message ="failed to update cube mdx, message:"+ex.Message.ToString(),Status=0 }
                };
            }
            return buildCube;
        }

    }
}
