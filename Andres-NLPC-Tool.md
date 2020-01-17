# Natural Language Processing and Collection Tool (NLPC Tool) Idea

NLPC tool would be a tool that can facilitate evidence gathering and gauging impact on investor behavior and financial impact. The tool would be a package that would hold a set of ESG word libraries and functions that standardize files and extracts relevant content and tags it with .
___

## Processing:
Feed diverse documentation through a single pipeline that transforms documentation into pliable text. This can include regulatory filing, sustainability reports, proxy resolutions, earning calls, prospectus, and other ESG related documentation.

Example: Selecting an earning call in the interface of tool next to the attached document would prompt tool to use otter.ai (a free transcribing service)  for transcribing earning calls which can then be processed through SASB natural language collection model (described below).

## Collecting:
Extract phrases based on match with a word library specific to the topic being investigated so that analyst (and potentially tool) can focus on areas of interest. Phrases would be tagged on the topic and level of relevance.

Example: Once all documentation has been processed, users would select the industry and dimension (can include multiple) that is being investigated. In this case, a consumer goods company that has issues in human capital and environment dimensions. The model would then look into the human capital and environment word libraries and extract phrases (or paragraphs) that match and tag them with "high" or "medium" relevance. Then it would look into a larger SASB library which would similarly extract phrases (or paragraphs) that match but only tag them with "low" relevance.
 
## Evaluate:
Analysts (or another node in the model) will then take (preferably) a top-down approach through which phrases with high relevance are evaluated first and then medium and low relevance cases. Possibly, report can also point to potential financial and investor impact that can help guide analyst in their analysis
 