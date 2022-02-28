﻿using CSVComparison;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace CSVComparisonTests
{
    public class TestComparisonUtils
    {
        [Test]
        public void TestSingleFileComparison()
        {       
            Console.WriteLine($"{Directory.GetCurrentDirectory()} - {Path.GetTempPath()} - {Path.GetTempFileName()}"); // + TestData

            var expectedPath = Path.Combine(Directory.GetCurrentDirectory(), "TestData", "ComplexReferenceFile.csv");
            var candidatePath = Path.Combine(Directory.GetCurrentDirectory(), "TestData", "ComplexCandidateFile.csv");

            var outputPath = Path.Combine(Path.GetTempPath(), "Output");
            var comparisonDefinitionPath = Path.Combine(Directory.GetCurrentDirectory(), "TestData", "Definition.xml");

            try
            {
                ComparisonUtils.RunSingleComparison(comparisonDefinitionPath, expectedPath, candidatePath, outputPath);

                Assert.IsTrue(File.Exists(Path.Combine(outputPath, "ComparisonResults.BREAKS.csv")));
            }
            finally
            {
                if (Directory.Exists(outputPath))
                {
                    Directory.Delete(outputPath, true);
                }
            }
        }

        [Test]
        public void TestFolderComparison()
        {
            Console.WriteLine($"{Directory.GetCurrentDirectory()} - {Path.GetTempPath()} - {Path.GetTempFileName()}"); // + TestData

            var expectedPath = Path.Combine(Path.GetTempPath(), "ExpectedTest");
            if (!Directory.Exists(expectedPath))
            {
                Directory.CreateDirectory(expectedPath);
            }

            var candidatePath = Path.Combine(Path.GetTempPath(), "CandidateTest");
            if (!Directory.Exists(candidatePath))
            {
                Directory.CreateDirectory(candidatePath);
            }

            var outputPath = Path.Combine(Path.GetTempPath(), "Output");

            var expectedFile = Path.Combine(expectedPath, "ComplexReferenceFile.csv");
            var candidateFile = Path.Combine(candidatePath, "ComplexCandidateFile.csv");
            File.Copy(Path.Combine(Directory.GetCurrentDirectory(), "TestData", "ComplexReferenceFile.csv"), expectedFile, true);
            File.Copy(Path.Combine(Directory.GetCurrentDirectory(), "TestData", "ComplexCandidateFile.csv"), candidateFile, true);

            var comparisonDefinitionPath = Path.Combine(Directory.GetCurrentDirectory(), "TestData", "MultipleDefinition.xml");
            try
            {
                ComparisonUtils.RunDirectoryComparison(comparisonDefinitionPath, expectedPath, candidatePath, outputPath);
                Assert.IsTrue(File.Exists(Path.Combine(outputPath, "Reconciliation-Results-Test.BREAKS.csv")));
            }
            finally
            {
                if (Directory.Exists(expectedPath))
                {
                    Directory.Delete(expectedPath, true);
                }

                if (Directory.Exists(candidatePath))
                {
                    Directory.Delete(candidatePath, true);
                }

                if (Directory.Exists(outputPath))
                {
                    Directory.Delete(outputPath, true);
                }
            }
        }
    }
}