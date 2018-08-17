using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace SSASRestfulAPI.Models
{
    public class UpdateMDXCalculation
    {
        private String _clrSSASConfigurationConnectionString;
        public String clrSSASConfigurationConnectionString
        {
            get { return this._clrSSASConfigurationConnectionString; }
            set { this._clrSSASConfigurationConnectionString = value; }
        }

        private String _mdxCaculationScript;
        public String mdxCaculationScript
        {
            get { return this._mdxCaculationScript; }
            set { this._mdxCaculationScript = value; }
        }

        private String _clrOlapServer;
        public String clrOlapServer
        {
            get { return this._clrOlapServer; }
            set { this._clrOlapServer = value; }
        }

        private String _clrOlapCubeDB;
        public String clrOlapCubeDB
        {
            get { return this._clrOlapCubeDB; }
            set { this._clrOlapCubeDB = value; }
        }

        private String _clrOlapCube;
        public String clrOlapCube
        {
            get { return this._clrOlapCube; }
            set { this._clrOlapCube = value; }
        }
    }
}