﻿using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Xml;
using System.Xml.Serialization;

namespace CSVComparison
{
    class Program
    {
        private static bool AppendFile = false;

        static void Main(string[] args)
        {
            if (args.Length < 3)
            {
                Console.WriteLine($"Invalid number of arguments {args.Length}. Expecting at least 3");
                Console.WriteLine("Arguments used:");
                foreach(string arg in args)
                {
                    Console.WriteLine(arg);
                }

                Console.WriteLine("Usage: CsVComparison ReferenceFilePath CandidateFilePath ConfigurationFilePath [OutputFile]");
                return;
            }

            var referenceFilePath = args[0];
            var candidateFilePath = args[1]; 
            var configurationFilePath = args[2];

            string outputFile = "";
            if (args.Length > 3)
            {
                outputFile = args[3];
            }

            if (Directory.Exists(referenceFilePath))
            {
                // This is a directory
                RunDirectoryComparison(configurationFilePath, referenceFilePath, candidateFilePath, outputFile);
            }
            else
            {
                // Default to single file comparison
                RunSingleComparison(configurationFilePath, referenceFilePath, candidateFilePath, outputFile);
            }
        }

        private static void RunDirectoryComparison(string configurationFilePath, string referenceFilePath, string candidateFilePath, string outputFile)
        {
            var xmlDocument = new XmlDocument();
            xmlDocument.Load(configurationFilePath);

            var xmlSerializer = new XmlSerializer(typeof(MultipleComparisonDefinition));
            var multiComparisonDefinition = (MultipleComparisonDefinition)xmlSerializer.Deserialize(new XmlNodeReader((XmlNode)xmlDocument.DocumentElement));

            // Now enumerate directory
            var referenceDirectory = new DirectoryInfo(referenceFilePath);
            AppendFile = false;

            foreach (var file in referenceDirectory.GetFiles())
            {
                var stopwatch = new Stopwatch();
                stopwatch.Start();

                // Get the comparisondefinition for the file, using the pattern
                var comparisonDefinitionForFileType = multiComparisonDefinition.FileComparisonDefinitions.Where(x => Regex.IsMatch(file.Name, x.FilePattern));

                if (comparisonDefinitionForFileType.Count() != 1)
                {
                    Console.WriteLine($"No valid Comparison Definition found for {file.FullName}");
                    continue;
                }

                var fileComparisonDefinition = comparisonDefinitionForFileType.First();

                Console.WriteLine($"Found Comparison Definition: {fileComparisonDefinition.Key}");
                var csvComparer = new CSVComparer(fileComparisonDefinition.ComparisonDefinition);

                // Search for candidate file. Try exact file match first, then try filepattern match
                ComparisonResult comparisonResult;
                if (File.Exists(Path.Combine(candidateFilePath, file.Name)))
                {
                    comparisonResult = csvComparer.CompareFiles(file.FullName, Path.Combine(candidateFilePath, file.Name));
                }
                else
                {
                    var directoryInfo = new DirectoryInfo(candidateFilePath);
                    var regex = new Regex(fileComparisonDefinition.FilePattern);
                    var candidatePaths = directoryInfo.GetFiles().Where(candidateFile => regex.IsMatch(candidateFile.Name));

                    if (candidatePaths.Count() != 1)
                    {
                        Console.WriteLine($"Unable to find a single matching file to compare with {file.FullName}. Found {candidatePaths.Count()}");
                        continue;
                    }

                    comparisonResult = csvComparer.CompareFiles(file.FullName, candidatePaths.First().FullName);
                }

                stopwatch.Stop();
                var elapsedTime = stopwatch.ElapsedMilliseconds;
                HandleResult(comparisonResult, elapsedTime, fileComparisonDefinition, outputFile);
                Console.WriteLine($"Comparison took {stopwatch.ElapsedMilliseconds}ms");
            }

            Console.WriteLine("Finished.");
        }

        private static void RunSingleComparison(string configurationFilePath, string referenceFilePath, string candidateFilePath, string outputFile)
        {
            var xmlDocument = new XmlDocument();
            xmlDocument.Load(configurationFilePath);

            var xmlSerializer = new XmlSerializer(typeof(ComparisonDefinition));
            var comparisonDefinition = (ComparisonDefinition)xmlSerializer.Deserialize(new XmlNodeReader((XmlNode)xmlDocument.DocumentElement));

            var stopwatch = new Stopwatch();
            stopwatch.Start();
            var csvComparer = new CSVComparer(comparisonDefinition);
            var comparisonResult = csvComparer.CompareFiles(referenceFilePath, candidateFilePath);
            stopwatch.Stop();

            Console.WriteLine($"Reference: {comparisonResult.ReferenceSource}");
            Console.WriteLine($"Candidate: {comparisonResult.CandidateSource}");

            if (string.IsNullOrEmpty(outputFile))
            {
                foreach (var breakResult in comparisonResult.BreakDetails)
                {
                    Console.WriteLine(breakResult.ToString());
                }
            }
            else
            {
                Console.WriteLine($"Saving results to {outputFile}");
                AppendFile = false;
                SaveResults(outputFile, comparisonResult, comparisonDefinition, stopwatch.ElapsedMilliseconds);
            }

            Console.WriteLine($"Finished. Comparison took {stopwatch.ElapsedMilliseconds}ms");
        }

        private static void HandleResult(ComparisonResult comparisonResult, long elapsedTime, FileComparisonDefinition fileComparisonDefinition, string outputFile)
        {
            Console.WriteLine($"Reference: {comparisonResult.ReferenceSource}");
            Console.WriteLine($"Candidate: {comparisonResult.CandidateSource}");

            if (comparisonResult.BreakDetails.Count() == 0)
            {
                Console.WriteLine("No differences found.");
            }

            if (string.IsNullOrEmpty(outputFile))
            {
                foreach (var breakResult in comparisonResult.BreakDetails)
                {
                    Console.WriteLine(breakResult.ToString());
                }
            }
            else
            {
                var resultsFile = "";
                if (Directory.Exists(outputFile))
                {
                    resultsFile = Path.Combine(outputFile, $"Reconciliation-Results-{fileComparisonDefinition.Key}.csv");
                }
                else
                {
                    resultsFile = outputFile;
                }

                Console.WriteLine($"Saving results to {resultsFile}");
                SaveResults(resultsFile, comparisonResult, fileComparisonDefinition.ComparisonDefinition, elapsedTime);
                if (!AppendFile)
                {
                    AppendFile = true;
                }
            }
        }


        private static void SaveResults(string outputFile, ComparisonResult comparisonResult, ComparisonDefinition comparisonDefinition, long elapsedMillis)
        {
            using (var sw = new StreamWriter(outputFile, AppendFile))
            {
                var xmlSerializer = new XmlSerializer(typeof(ComparisonDefinition));
                xmlSerializer.Serialize(sw, comparisonDefinition);
                sw.WriteLine();
                sw.WriteLine($"Reference: {comparisonResult.ReferenceSource}");
                sw.WriteLine($"Candidate: {comparisonResult.CandidateSource}");
                sw.WriteLine($"Comparison took {elapsedMillis}ms");
                sw.WriteLine($"Number of breaks {comparisonResult.BreakDetails.Count()}");
                sw.WriteLine("Break Type,Key,Reference Row, Reference Value, Candidate Row, Candidate Value");
                foreach(var breakResult in comparisonResult.BreakDetails)
                {
                    sw.WriteLine($"{breakResult.BreakType},{breakResult.BreakKey},{breakResult.ReferenceRow},{breakResult.ReferenceValue},{breakResult.CandidateRow},{breakResult.CandidateValue}");
                }
            }
        }
    }
}