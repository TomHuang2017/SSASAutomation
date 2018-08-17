using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace SSASRestfulAPI.Models
{
    public class BuildSSASCubeWithConfiguration
    {
        private String _clrDWServer;
        public String clrDWServer
        {
            get { return this._clrDWServer; }
            set { this._clrDWServer = value; }
        }

        private String _clrDWName;
        public String clrDWName
        {
            get { return this._clrDWName; }
            set { this._clrDWName = value; }
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

        private String _clrDWConnectionString;
        public String clrDWConnectionString
        {
            get { return this._clrDWConnectionString; }
            set { this._clrDWConnectionString = value; }
        }

        private String _clrSSASConfigurationConnectionString;
        public String clrSSASConfigurationConnectionString
        {
            get { return this._clrSSASConfigurationConnectionString; }
            set { this._clrSSASConfigurationConnectionString = value; }
        }
    }
}