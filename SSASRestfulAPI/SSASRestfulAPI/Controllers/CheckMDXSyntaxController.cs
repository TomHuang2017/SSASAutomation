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
    public class CheckMDXSyntaxController : ApiController
    {
        MessageStatus[] buildCube = new MessageStatus[] 
        { 
            new MessageStatus { Message = "MDX syntax is valid",Status=1}
        };

        //POST: /api/CheckMDXSyntax
        public IEnumerable<MessageStatus> CheckMDXSyntax([FromBody]CheckMDXSyntax Parameters)
        {
            try
            {
                MDXSyntaxInfo info = new MDXSyntaxInfo();
                info = AS_API_HELPER.checkMDXSyntax(Parameters.mdxCaculationScript);
                if (info.IsValid)
                {
                    buildCube = new MessageStatus[] 
                    { 
                        new MessageStatus { Message = "MDX syntax is valid",Status=1}
                    };
                }
                else
                {
                    buildCube = new MessageStatus[] 
                    { 
                        new MessageStatus { Message =info.SyntaxErrorMessage,Status=0,Column=info.Column,Line=info.Line}
                    };
                }

            }
            catch (Exception ex)
            {
                buildCube = new MessageStatus[] 
                    { 
                        new MessageStatus { Message = "failed to check mdx syntax, message:"+ex.Message.ToString(),Status=0}
                    };
            }
            return buildCube;
        }

    }
}
