using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ESB_ConnectionPoints.PluginsInterfaces;
using ESB_ConnectionPoints.Utils;

namespace ESB.Patio.S3
{
    class OutgoingConnectionPointFactory : IOutgoingConnectionPointFactory
    {
        public IOutgoingConnectionPoint Create(Dictionary<string, string> parameters, IServiceLocator serviceLocator)
        {
            return (IOutgoingConnectionPoint)new OutgoingConnectionPoint(parameters.GetStringParameter("Настройки исходящей точки"), serviceLocator);
        }
    }
}
