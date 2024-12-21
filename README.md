# Hadoop MapReduce Data Analysis Projects

This repository contains a collection of MapReduce programs implemented in Java for various data analysis tasks. The project includes four distinct exercises that demonstrate different aspects of distributed data processing using Hadoop.

## Table of Contents
- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Project Structure](#project-structure)
- [Exercises](#exercises)
    - [1. Numeronym Analysis](#1-numeronym-analysis)
    - [2. Movie Data Analysis](#2-movie-data-analysis)
    - [3. DNA Sequence Analysis](#3-dna-sequence-analysis)
    - [4. Graph Degree Analysis](#4-graph-degree-analysis)
- [Installation and Setup](#installation-and-setup)
- [Usage](#usage)

## Overview

This project showcases various MapReduce implementations for processing and analyzing different types of data:
- Text processing and numeronym generation
- Movie database analysis
- DNA sequence pattern recognition
- Graph analysis with probabilistic edges

## Prerequisites

- Java Development Kit (JDK)
- Hadoop environment
- Basic understanding of MapReduce paradigm
- Maven or similar build tool (optional)

## Project Structure

```
├── Exercise1/
│   ├── NumeronymCount.java
│   └── Numeronyms_output.txt
├── Exercise2/
│   ├── MovieDurationCountry.java
│   └── MovieGenreYearCount.java
├── Exercise3/
│   ├── DNAMerCount.java
│   └── DNA_output.txt
└── Exercise4/
    └── Main.java
```

## Exercises

### 1. Numeronym Analysis

A MapReduce program that generates and counts numeronyms from text input. A numeronym is formed using the first and last characters of a word with the number of characters in between.

**Features:**
- Ignores words shorter than 3 characters
- Removes punctuation
- Converts text to lowercase
- Configurable minimum occurrence threshold

**Usage:**
```bash
javac -cp $(hadoop classpath) Exercise1/NumeronymCount.java
jar cvf movie-duration.jar Exercise1/NumeronymCount*.class
hadoop jar movie-duration.jar Exercise1.NumeronymCount [threshold]
```

### 2. Movie Data Analysis

Two separate MapReduce programs for analyzing movie data:

1. **MovieDurationCountry**: Calculates total movie duration per country
2. **MovieGenreYearCount**: Counts movies by year and genre with IMDb rating > 8.0

**Usage:**
```bash
# For MovieDurationCountry
javac -cp $(hadoop classpath) Exercise2/MovieDurationCountry.java
jar cvf movie-duration.jar Exercise2/MovieDurationCountry*.class
hadoop jar movie-duration.jar Exercise2.MovieDurationCountry

# For MovieGenreYearCount
javac -cp $(hadoop classpath) Exercise2/MovieGenreYearCount.java
jar cvf movie-genre.jar Exercise2/MovieGenreYearCount*.class
hadoop jar movie-genre.jar Exercise2.MovieGenreYearCount
```

### 3. DNA Sequence Analysis

Analyzes DNA sequences by counting occurrences of:
- 2-mers (e.g., AT, TC)
- 3-mers (e.g., AAC, TCC)
- 4-mers (e.g., TGCT, CCAG)

**Usage:**
```bash
javac -cp $(hadoop classpath) Exercise3/DNAMerCount.java
jar cvf movie-duration.jar Exercise3/DNAMerCount*.class
hadoop jar movie-duration.jar Exercise3.DNAMerCount
```

### 4. Graph Degree Analysis

A two-phase MapReduce program that:
1. Calculates vertex degrees in a probabilistic graph
2. Filters vertices based on average degree threshold

**Features:**
- Configurable probability threshold
- Two-phase MapReduce implementation
- Average degree calculation using counters

**Usage:**
```bash
javac -cp $(hadoop classpath) Exercise4/Main.java
jar cvf movie-duration.jar Exercise4/Main*.class
hadoop jar movie-duration.jar Exercise4.Main [threshold]
```
Default threshold is 0.5 if not specified. Threshold must be between 0 and 1.

## Installation and Setup

1. Ensure Hadoop is properly installed and configured on your system
2. Clone this repository:
   ```bash
   git clone [repository-url]
   ```
3. Navigate to the project directory:
   ```bash
   cd [project-directory]
   ```
4. Compile and create JAR files as shown in the usage sections above

## Contributing

Feel free to submit issues and enhancement requests or contribute to the project. Any contributions you make are greatly appreciated.

## License

This project is licensed under the MIT License - see the LICENSE file for details.