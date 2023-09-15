namespace MongoDbSync;

internal static class Program
{

    static async Task Main(string[] args)
    {
        var sourceConnString = args[0];
        var targetConnString = args[1];
        var dbs = args[2];
        if (!(args.Length > 3 && int.TryParse(args[3], out var noOfHours)))
            noOfHours = 50;
        await new Sync(sourceConnString, targetConnString, dbs.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries), noOfHours).Execute();
    }
}