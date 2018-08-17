using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace SSASRestfulAPI.Models
{
    public class CheckMDXSyntax
    {

        private String _mdxCaculationScript;
        public String mdxCaculationScript
        {
            get { return this._mdxCaculationScript; }
            set { this._mdxCaculationScript = value; }
        }
        public CheckMDXSyntax(String __mdxCaculationScript)
        {
            this._mdxCaculationScript = __mdxCaculationScript;
        }
        
    }
}