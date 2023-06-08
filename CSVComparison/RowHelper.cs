using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CSVComparison
{
    public class RowHelper
	{
		public static List<string> SplitRowWithQuotes(ReadOnlySpan<char> line, ReadOnlySpan<char> delimiter)
		{
			var columnValues = new List<string>();
			Span<char> remaining = new Span<char>(line.ToArray());
			Span<char> nextQuote = new Span<char>("\"".ToArray());

			while (remaining.Length > 0)
			{
				var nextDelimiterIndex = remaining.IndexOf(delimiter);
				var nextQuoteIndex = remaining.IndexOf(nextQuote);

				if (nextDelimiterIndex == -1)
				{
					// Add the last characters on the row
					columnValues.Add(remaining.Slice(0).ToString());
					break;
				}
				else if (nextQuoteIndex == -1 || nextDelimiterIndex < nextQuoteIndex)
				{
					columnValues.Add(remaining.Slice(0,nextDelimiterIndex).ToString());
					if (remaining.Length == 1)
					{
						columnValues.Add("");
					}

					remaining = remaining.Slice(nextDelimiterIndex + delimiter.Length);
				}
				else if (nextQuoteIndex > -1)
				{
					bool isInQuoteBlock = true;
					var quote = new StringBuilder();

					while (isInQuoteBlock)
					{
						remaining = remaining.Slice(nextQuoteIndex + 1);
						var quoteIndex = remaining.IndexOf(nextQuote);
						if (quoteIndex == -1)
						{
							// There is no closing quote. This is invalid CSV so save out all the rest
							quote.Append($"\"{remaining.Slice(0).ToString()}");
							remaining = remaining.Slice(remaining.Length - 1, 0);
							break;
						}

						quote.Append($"\"{remaining.Slice(0,quoteIndex).ToString()}\"");
						remaining = remaining.Slice(quoteIndex + 1);
						if (remaining.IndexOf(nextQuote) != 0)
						{
							isInQuoteBlock = false;
						}
					}

					columnValues.Add(quote.ToString());
					if (remaining.Length > 0)
					{
						remaining = remaining.Slice(delimiter.Length);
					}
				}
			}

			return columnValues;
		}
	}
}