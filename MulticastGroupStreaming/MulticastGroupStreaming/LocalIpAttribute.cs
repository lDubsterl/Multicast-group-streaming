using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;
using System.Net;

namespace MulticastGroupStreaming
{
    [AttributeUsage(AttributeTargets.Method)]
    public class LocalIpAttribute : Attribute, IActionFilter
    {
        public void OnActionExecuting(ActionExecutingContext filterContext)
        {
            if (filterContext.HttpContext.Connection.RemoteIpAddress != null && !IPAddress.IsLoopback(filterContext.HttpContext.Connection.RemoteIpAddress))
                filterContext.Result = new UnauthorizedResult();
        }
        public void OnActionExecuted(ActionExecutedContext context) { }
    }
}
