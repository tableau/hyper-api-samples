using System;
using System.IO;

using Microsoft.Extensions.CommandLineUtils;

using Tableau.HyperAPI;

namespace Example
{
    public abstract class Example
    {
        public abstract void Execute(string dataDir);
    }

    public class Program
    {
        private static void FindDataDir(ref string dataDir)
        {
            if (string.IsNullOrEmpty(dataDir))
            {
                dataDir = "data";
                if (!Directory.Exists(dataDir))
                {
                    Console.WriteLine("could not find example data directory, use --data-dir to specify it");
                    Environment.Exit(1);
                }
            }
        }

        private static void AddExample(CommandLineApplication app, string name, string description, Example example, CommandOption dataDirOption)
        {
            app.Command(name, (cmd) =>
            {
                cmd.Description = description;
                cmd.HelpOption("-?|-h|--help");
                cmd.OnExecute(() =>
                {
                    string dataDir = dataDirOption.Value();
                    FindDataDir(ref dataDir);

                    try
                    {
                        example.Execute(dataDir);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex);
                        return 1;
                    }

                    return 0;
                });
            });
        }

        public static void Main(string[] args)
        {
            CommandLineApplication app = new CommandLineApplication();
            app.Description = "Extract API Example Application";
            app.HelpOption("-? | -h | --help");

            CommandOption dataDir = app.Option("--data-dir", "Path to the directory with the example data", CommandOptionType.SingleValue);

            AddExample(app, "create-extract-from-csv", "Load data from a CSV into a new Hyper file.", new CreateHyperFileFromCsv(), dataDir);
            AddExample(app, "delete-data-in-extract", "Delete data in an existing Hyper file.", new DeleteDataInExistingHyperFile(), dataDir);
            AddExample(app, "insert-data-into-multiple-tables", "Insert data into multiple tables.", new InsertDataIntoMultipleTables(), dataDir);
            AddExample(app, "insert-data-into-single-table", "Insert data into a single tables.", new InsertDataIntoSingleTable(), dataDir);
            AddExample(app, "insert-data-with-expressions", "Push down computations to Hyper during Insertion using expressions.", new InsertDataWithExpressions(), dataDir);
            AddExample(app, "insert-spatial-data-to-a-hyper-file", "Insert spatial data into a hyper file.", new InsertSpatialDataToAHyperFile(), dataDir);
            AddExample(app, "read-data-from-existing-extract", "Read data from an existing Hyper file.", new ReadDataFromExistingHyperFile(), dataDir);
            AddExample(app, "update-data-in-existing-extract", "Update data in an existing Hyper file.", new UpdateDataInExistingHyperFile(), dataDir);

            Environment.Exit(app.Execute(args));
        }
    }
}
