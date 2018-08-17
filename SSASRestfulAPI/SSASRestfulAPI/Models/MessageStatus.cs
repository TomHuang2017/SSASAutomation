using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace SSASRestfulAPI.Models
{
    public class MessageStatus
    {
        public int Status { get; set; }
        public string Message { get; set; }
        public Int32 Line { get; set; }
        public Int32 Column { get; set; }
    }
}