using NUnit.Framework;
using SqlDataCopier;

namespace Tests
{
    [TestFixture]
    public class Class1
    {
        [Test]
        public void Test1()
        {
            CopySqlDataCmdlet cmd = new CopySqlDataCmdlet
            {
                SourceConnectionString = "server=hippocampus;database=afs;integrated security=true",
                DestinationConnectionString = "server=.;database=afsx;integrated security=true",
                Tables = new[] { "truechecks.queries", "batch.queries", "truechecks.checkalerts" }
            };
            cmd.TestProcessRecord();
            
        }
    }
}
