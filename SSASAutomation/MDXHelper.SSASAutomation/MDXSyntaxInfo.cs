using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MDXHelper.SSASAutomation
{
    public class MDXSyntaxInfo
    {
        private String _SyntaxErrorMDX;
        public String SyntaxErrorMDX
        { get { return this._SyntaxErrorMDX; } set { this._SyntaxErrorMDX = value; } }

        private bool _IsValid;
        public bool IsValid
        { get { return this._IsValid; } set { this._IsValid = value; } }


        private String _SyntaxErrorMessage;
        public String SyntaxErrorMessage
        { get { return this._SyntaxErrorMessage; } set { this._SyntaxErrorMessage = value; } }

        private int _Column;
        public int Column
        { get { return this._Column; } set { this._Column = value; } }

        private int _Line;
        public int Line
        { get { return this._Line; } set { this._Line = value; } }

        private int _Length;
        public int Length
        { get { return this._Length; } set { this._Length = value; } }
    }
}
