import nbformat
import ast
import re
import os
import json
import networkx as nx
from collections import defaultdict, Counter
from typing import List, Dict, Any, Set, Tuple, Optional
import matplotlib.pyplot as plt
from IPython.display import display, HTML
from dataclasses import dataclass


@dataclass
class CodeEntityReference:
    """Class to track references to variables, functions, and classes."""
    name: str
    kind: str  # 'variable', 'function', 'class', 'import'
    defined_in_cell: int
    references: List[int] = None

    def __post_init__(self):
        if self.references is None:
            self.references = []


class AdvancedNotebookAnalyzer:
    """Advanced analyzer for Jupyter notebooks with visualization and dependency tracking."""

    def __init__(self, notebook_path: str):
        self.notebook_path = notebook_path
        self.notebook = None
        self.code_cells = []
        self.markdown_cells = []
        self.cell_dependencies = nx.DiGraph()
        self.entities = {}  # name -> CodeEntityReference
        self.framework_usage = defaultdict(list)
        self.magic_commands = defaultdict(int)
        self.advanced_patterns = defaultdict(list)

    def load_notebook(self) -> None:
        """Load the Jupyter notebook."""
        try:
            with open(self.notebook_path, 'r', encoding='utf-8') as f:
                self.notebook = nbformat.read(f, as_version=4)
            print(f"Successfully loaded notebook: {self.notebook_path}")

            # Separate code and markdown cells
            for i, cell in enumerate(self.notebook.cells):
                if cell.cell_type == 'code':
                    self.code_cells.append((i, cell))
                elif cell.cell_type == 'markdown':
                    self.markdown_cells.append((i, cell))

        except Exception as e:
            print(f"Error loading notebook: {e}")
            raise

    def analyze_notebook(self) -> Dict[str, Any]:
        """
        Perform comprehensive analysis of the notebook.

        Returns:
            Dictionary with analysis results
        """
        if not self.notebook:
            self.load_notebook()

        results = {
            "general_info": self._analyze_general_info(),
            "code_summary": self._analyze_code_cells(),
            "markdown_summary": self._analyze_markdown_cells(),
            "dependencies": self._analyze_dependencies(),
            "frameworks": self._analyze_frameworks(),
            "workflows": self._analyze_workflows(),
            "cell_clusters": self._cluster_cells(),
            "execution_flow": self._analyze_execution_flow()
        }

        return results

    def _analyze_general_info(self) -> Dict[str, Any]:
        """Analyze general notebook information."""
        metadata = self.notebook.metadata

        # Extract kernel info
        kernel_info = {
            "name": metadata.get('kernelspec', {}).get('name', 'Unknown'),
            "language": metadata.get('kernelspec', {}).get('language', 'Unknown')
        }

        # Count cell types
        cell_counts = Counter(cell.cell_type for cell in self.notebook.cells)

        # Analyze notebook structure
        structure_info = {
            "total_cells": len(self.notebook.cells),
            "code_cells": cell_counts.get('code', 0),
            "markdown_cells": cell_counts.get('markdown', 0),
            "raw_cells": cell_counts.get('raw', 0),
            "markdown_code_ratio": cell_counts.get('markdown', 0) / max(cell_counts.get('code', 1), 1),
            "has_outputs": any(
                len(cell.get('outputs', [])) > 0 for cell in self.notebook.cells if cell.cell_type == 'code')
        }

        return {
            "notebook_name": os.path.basename(self.notebook_path),
            "kernel": kernel_info,
            "structure": structure_info,
            "language_info": metadata.get('language_info', {}),
            "has_widgets": 'widgets' in metadata
        }

    def _analyze_code_cells(self) -> Dict[str, Any]:
        """Analyze code cells in the notebook."""
        total_lines = 0
        non_empty_cells = 0
        imports = []
        definitions = {
            "functions": [],
            "classes": []
        }

        for cell_idx, cell in self.code_cells:
            code = cell.source

            # Count lines
            if code.strip():
                non_empty_cells += 1
                total_lines += len(code.splitlines())

            # Analyze code with AST
            try:
                tree = ast.parse(code)

                # Track imports
                for node in ast.iter_child_nodes(tree):
                    if isinstance(node, ast.Import):
                        for name in node.names:
                            imports.append(name.name)
                            self.entities[name.name] = CodeEntityReference(
                                name=name.name,
                                kind='import',
                                defined_in_cell=cell_idx
                            )
                    elif isinstance(node, ast.ImportFrom):
                        module = node.module if node.module else ''
                        for name in node.names:
                            import_name = f"{module}.{name.name}" if module else name.name
                            imports.append(import_name)
                            self.entities[import_name] = CodeEntityReference(
                                name=import_name,
                                kind='import',
                                defined_in_cell=cell_idx
                            )
                    elif isinstance(node, ast.FunctionDef):
                        definitions["functions"].append({
                            "name": node.name,
                            "cell_index": cell_idx,
                            "args": [arg.arg for arg in node.args.args],
                            "decorators": [self._get_decorator_name(d) for d in node.decorator_list]
                        })
                        self.entities[node.name] = CodeEntityReference(
                            name=node.name,
                            kind='function',
                            defined_in_cell=cell_idx
                        )
                    elif isinstance(node, ast.ClassDef):
                        definitions["classes"].append({
                            "name": node.name,
                            "cell_index": cell_idx,
                            "bases": [self._get_base_name(base) for base in node.bases],
                            "methods": [m.name for m in node.body if isinstance(m, ast.FunctionDef)]
                        })
                        self.entities[node.name] = CodeEntityReference(
                            name=node.name,
                            kind='class',
                            defined_in_cell=cell_idx
                        )

                # Find magic commands
                self._find_magic_commands(code, cell_idx)

            except SyntaxError:
                # Skip cells with syntax errors
                pass

        # Analyze variable usage and track dependencies
        for cell_idx, cell in self.code_cells:
            code = cell.source
            self._track_variable_usage(code, cell_idx)

        return {
            "code_stats": {
                "total_lines": total_lines,
                "avg_lines_per_cell": total_lines / max(non_empty_cells, 1),
                "non_empty_cells": non_empty_cells
            },
            "imports": sorted(set(imports)),
            "top_imports": Counter(i.split('.')[0] for i in imports).most_common(5),
            "definitions": definitions,
            "magic_commands": dict(self.magic_commands)
        }

    def _get_decorator_name(self, node: ast.expr) -> str:
        """Get the name of a decorator."""
        if isinstance(node, ast.Name):
            return node.id
        elif isinstance(node, ast.Attribute):
            if isinstance(node.value, ast.Name):
                return f"{node.value.id}.{node.attr}"
        return "complex_decorator"

    def _get_base_name(self, node: ast.expr) -> str:
        """Get the name of a base class."""
        if isinstance(node, ast.Name):
            return node.id
        elif isinstance(node, ast.Attribute):
            if isinstance(node.value, ast.Name):
                return f"{node.value.id}.{node.attr}"
        return "complex_base"

    def _find_magic_commands(self, code: str, cell_idx: int) -> None:
        """Find and analyze magic commands in cell."""
        magic_pattern = r'^%(\w+)|^%%(\w+)'

        for line in code.splitlines():
            match = re.match(magic_pattern, line.strip())
            if match:
                magic = match.group(1) or match.group(2)
                self.magic_commands[magic] += 1

                # Track specific magic commands
                if magic in ['matplotlib', 'pylab', 'bokeh']:
                    self.framework_usage['visualization'].append((cell_idx, magic))
                elif magic in ['time', 'timeit']:
                    self.framework_usage['performance'].append((cell_idx, magic))
                elif magic in ['sql', 'bigquery', 'postgres']:
                    self.framework_usage['database'].append((cell_idx, magic))

    def _track_variable_usage(self, code: str, cell_idx: int) -> None:
        """Track variable definitions and usage."""
        try:
            tree = ast.parse(code)

            # Find defined variables
            defined_vars = set()
            for node in ast.walk(tree):
                if isinstance(node, ast.Assign):
                    for target in node.targets:
                        if isinstance(target, ast.Name):
                            defined_vars.add(target.id)
                            if target.id not in self.entities:
                                self.entities[target.id] = CodeEntityReference(
                                    name=target.id,
                                    kind='variable',
                                    defined_in_cell=cell_idx
                                )

            # Find referenced variables
            for node in ast.walk(tree):
                if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
                    var_name = node.id
                    if var_name in self.entities and var_name not in defined_vars:
                        self.entities[var_name].references.append(cell_idx)
                        # Add dependency edge if reference is in a different cell
                        if self.entities[var_name].defined_in_cell != cell_idx:
                            self.cell_dependencies.add_edge(
                                self.entities[var_name].defined_in_cell,
                                cell_idx
                            )
        except SyntaxError:
            # Skip cells with syntax errors
            pass

    def _analyze_markdown_cells(self) -> Dict[str, Any]:
        """Analyze markdown cells in the notebook."""
        results = {
            "headers": [],
            "links": [],
            "has_math": False,
            "has_tables": False,
            "has_images": False
        }

        for cell_idx, cell in self.markdown_cells:
            content = cell.source

            # Find headers
            header_pattern = r'^(#+)\s+(.+)$'
            for line in content.splitlines():
                match = re.match(header_pattern, line)
                if match:
                    level = len(match.group(1))
                    text = match.group(2).strip()
                    results["headers"].append({
                        "level": level,
                        "text": text,
                        "cell_index": cell_idx
                    })

            # Find links
            link_pattern = r'\[([^\]]+)\]\(([^)]+)\)'
            for match in re.finditer(link_pattern, content):
                results["links"].append({
                    "text": match.group(1),
                    "url": match.group(2),
                    "cell_index": cell_idx
                })

            # Check for math, tables and images
            if '$$' in content or '$' in content or '\\begin{' in content:
                results["has_math"] = True

            if '|--' in content or '|-:' in content or '|:-' in content:
                results["has_tables"] = True

            if '![' in content:
                results["has_images"] = True

        return results

    def _analyze_dependencies(self) -> Dict[str, Any]:
        """Analyze dependencies between cells and entities."""
        # Compute centrality measures for cells
        if len(self.cell_dependencies) > 0:
            centrality = nx.degree_centrality(self.cell_dependencies)
            betweenness = nx.betweenness_centrality(self.cell_dependencies)

            key_cells = sorted(centrality.items(), key=lambda x: x[1], reverse=True)[:5]
        else:
            centrality = {}
            betweenness = {}
            key_cells = []

        # Analyze entity dependencies
        entity_stats = {
            "variables": sum(1 for e in self.entities.values() if e.kind == 'variable'),
            "functions": sum(1 for e in self.entities.values() if e.kind == 'function'),
            "classes": sum(1 for e in self.entities.values() if e.kind == 'class'),
            "imports": sum(1 for e in self.entities.values() if e.kind == 'import'),
            "most_referenced": []
        }

        # Find most referenced entities
        most_ref = sorted(
            [e for e in self.entities.values() if e.references],
            key=lambda x: len(x.references),
            reverse=True
        )[:10]

        entity_stats["most_referenced"] = [
            {"name": e.name, "kind": e.kind, "references": len(e.references)}
            for e in most_ref
        ]

        return {
            "cell_dependencies": {
                "edges": list(self.cell_dependencies.edges()),
                "key_cells": key_cells,
                "centrality": centrality,
                "betweenness": betweenness
            },
            "entity_stats": entity_stats
        }

    def _analyze_frameworks(self) -> Dict[str, Any]:
        """Analyze the usage of frameworks and libraries."""
        framework_patterns = {
            "pandas": r'pd\.|pandas|DataFrame|Series',
            "numpy": r'np\.|numpy',
            "matplotlib": r'plt\.|matplotlib',
            "seaborn": r'sns\.|seaborn',
            "scikit-learn": r'sklearn|from sklearn',
            "tensorflow": r'tf\.|tensorflow|keras',
            "pytorch": r'torch|nn\.',
            "spark": r'spark|pyspark|SparkContext|SparkSession',
            "nltk": r'nltk',
            "spacy": r'spacy|nlp\(',
            "transformers": r'transformers|from transformers',
            "dask": r'dask',
            "networkx": r'nx\.|networkx',
            "scipy": r'scipy|from scipy',
            "statsmodels": r'statsmodels|sm\.',
            "xgboost": r'xgboost|XGBClassifier|XGBRegressor',
            "lightgbm": r'lightgbm|LGBMClassifier|LGBMRegressor',
            "plotly": r'plotly|px\.',
            "bokeh": r'bokeh',
            "dash": r'dash',
            "fastai": r'fastai',
            "jax": r'jax',
            "ray": r'ray',
            "opencv": r'cv2\.'
        }

        framework_usage = defaultdict(list)

        for cell_idx, cell in self.code_cells:
            code = cell.source
            for framework, pattern in framework_patterns.items():
                if re.search(pattern, code):
                    framework_usage[framework].append(cell_idx)

        # Categorize frameworks
        categories = {
            "data_processing": ["pandas", "numpy", "dask"],
            "visualization": ["matplotlib", "seaborn", "plotly", "bokeh", "dash"],
            "machine_learning": ["scikit-learn", "xgboost", "lightgbm", "statsmodels"],
            "deep_learning": ["tensorflow", "pytorch", "keras", "fastai", "jax"],
            "nlp": ["nltk", "spacy", "transformers"],
            "distributed": ["spark", "dask", "ray"],
            "other": ["networkx", "scipy", "opencv"]
        }

        categorized = {cat: [fw for fw in fws if fw in framework_usage]
                       for cat, fws in categories.items()}

        # Detect patterns for specific frameworks
        self._detect_framework_specific_patterns()

        return {
            "framework_usage": {k: v for k, v in framework_usage.items() if v},
            "categorized": {k: v for k, v in categorized.items() if v},
            "advanced_patterns": dict(self.advanced_patterns)
        }

    def _detect_framework_specific_patterns(self) -> None:
        """Detect specific usage patterns for common frameworks."""
        for cell_idx, cell in self.code_cells:
            code = cell.source

            # Pandas patterns
            if re.search(r'pd\.|pandas', code):
                if re.search(r'read_csv|read_excel|read_parquet|read_json', code):
                    self.advanced_patterns['data_loading'].append(cell_idx)
                if re.search(r'groupby|pivot|merge|join|concat', code):
                    self.advanced_patterns['data_aggregation'].append(cell_idx)
                if re.search(r'fillna|dropna|replace|drop_duplicates', code):
                    self.advanced_patterns['data_cleaning'].append(cell_idx)

            # Scikit-learn patterns
            if re.search(r'sklearn', code):
                if re.search(r'train_test_split', code):
                    self.advanced_patterns['data_splitting'].append(cell_idx)
                if re.search(r'fit|predict', code):
                    self.advanced_patterns['model_training'].append(cell_idx)
                if re.search(r'cross_val|GridSearchCV|RandomizedSearchCV', code):
                    self.advanced_patterns['hyperparameter_tuning'].append(cell_idx)

            # Tensorflow/Keras patterns
            if re.search(r'tensorflow|keras|tf\.', code):
                if re.search(r'Sequential|Model', code):
                    self.advanced_patterns['neural_network_definition'].append(cell_idx)
                if re.search(r'compile|fit', code):
                    self.advanced_patterns['deep_learning_training'].append(cell_idx)

            # PyTorch patterns
            if re.search(r'torch|nn\.', code):
                if re.search(r'class.*\(nn\.Module\)', code):
                    self.advanced_patterns['neural_network_definition'].append(cell_idx)
                if re.search(r'backward|step', code):
                    self.advanced_patterns['deep_learning_training'].append(cell_idx)

            # PySpark patterns
            if re.search(r'spark|pyspark', code):
                if re.search(r'createDataFrame|read\.|load', code):
                    self.advanced_patterns['distributed_data_loading'].append(cell_idx)
                if re.search(r'groupBy|agg|join|select|filter', code):
                    self.advanced_patterns['distributed_data_processing'].append(cell_idx)

            # NLP patterns
            if re.search(r'nltk|spacy|transformers', code):
                if re.search(r'tokenize|lemma|stem|pos_tag', code):
                    self.advanced_patterns['text_preprocessing'].append(cell_idx)
                if re.search(r'embedding|vectorize|tfidf|word2vec|glove', code):
                    self.advanced_patterns['text_vectorization'].append(cell_idx)

            # Visualization patterns
            if re.search(r'plt\.|seaborn|plotly|bokeh', code):
                if re.search(r'plot|scatter|hist|bar|boxplot|heatmap', code):
                    self.advanced_patterns['data_visualization'].append(cell_idx)

    def _analyze_workflows(self) -> Dict[str, Any]:
        """Analyze if the notebook follows common data science workflows."""
        workflow_stages = {
            "data_loading": self.advanced_patterns.get('data_loading', []),
            "data_exploration": [],
            "data_cleaning": self.advanced_patterns.get('data_cleaning', []),
            "feature_engineering": [],
            "model_building": self.advanced_patterns.get('model_training', []) +
                              self.advanced_patterns.get('neural_network_definition', []),
            "model_evaluation": [],
            "model_deployment": []
        }

        # Check for data exploration (usually includes describe, info, shape, head, etc.)
        for cell_idx, cell in self.code_cells:
            code = cell.source
            if re.search(r'\.describe\(\)|\.info\(\)|\.shape|\.head\(\)|\.tail\(\)|\.value_counts\(\)', code):
                workflow_stages["data_exploration"].append(cell_idx)

            # Check for feature engineering
            if re.search(r'feature|transform|scaling|normalization|StandardScaler|MinMaxScaler|OneHotEncoder', code):
                workflow_stages["feature_engineering"].append(cell_idx)

            # Check for model evaluation
            if re.search(
                    r'accuracy_score|precision_score|recall_score|f1_score|confusion_matrix|classification_report|mean_squared_error|r2_score',
                    code):
                workflow_stages["model_evaluation"].append(cell_idx)

            # Check for model deployment
            if re.search(r'save_model|joblib.dump|pickle.dump|export|deploy|serving|prediction|inference', code):
                workflow_stages["model_deployment"].append(cell_idx)

        # Determine workflow completeness
        workflow_exists = {k: len(v) > 0 for k, v in workflow_stages.items()}
        completeness = sum(workflow_exists.values()) / len(workflow_exists)

        # Check workflow order
        ordered = True
        last_idx = -1
        for stage in ["data_loading", "data_exploration", "data_cleaning",
                      "feature_engineering", "model_building", "model_evaluation",
                      "model_deployment"]:
            if workflow_stages[stage]:
                min_idx = min(workflow_stages[stage])
                if min_idx < last_idx:
                    ordered = False
                last_idx = min_idx

        return {
            "workflow_stages": workflow_stages,
            "workflow_exists": workflow_exists,
            "workflow_completeness": completeness,
            "ordered_workflow": ordered
        }

    def _cluster_cells(self) -> Dict[str, Any]:
        """Cluster cells by their purpose."""
        clusters = defaultdict(list)

        for cell_idx, cell in self.code_cells:
            code = cell.source

            # Determine cell purpose
            if any(cell_idx in self.advanced_patterns.get(pattern, [])
                   for pattern in ['data_loading', 'distributed_data_loading']):
                clusters["data_loading"].append(cell_idx)

            elif any(cell_idx in self.advanced_patterns.get(pattern, [])
                     for pattern in ['data_cleaning', 'data_aggregation']):
                clusters["data_preparation"].append(cell_idx)

            elif any(cell_idx in self.advanced_patterns.get(pattern, [])
                     for pattern in ['model_training', 'neural_network_definition',
                                     'deep_learning_training']):
                clusters["modeling"].append(cell_idx)

            elif any(cell_idx in self.advanced_patterns.get(pattern, [])
                     for pattern in ['data_visualization']):
                clusters["visualization"].append(cell_idx)

            elif re.search(r'print|display|\.\w+\(\)', code):
                clusters["output_display"].append(cell_idx)

            elif re.search(r'def\s+\w+|class\s+\w+', code):
                clusters["utility_functions"].append(cell_idx)

            elif re.search(r'import\s+\w+|from\s+\w+\s+import', code):
                clusters["imports"].append(cell_idx)

            else:
                clusters["miscellaneous"].append(cell_idx)

        return dict(clusters)

    def _analyze_execution_flow(self) -> Dict[str, Any]:
        """Analyze the execution flow of the notebook."""
        cell_execution_order = []

        for i, cell in enumerate(self.notebook.cells):
            if cell.cell_type == 'code':
                execution_count = cell.get('execution_count')
                if execution_count is not None:
                    cell_execution_order.append((i, execution_count))

        # Sort by execution count
        cell_execution_order.sort(key=lambda x: x[1])

        # Check if cells were executed in order
        in_order = all(a <= b for a, b in zip([i for i, _ in cell_execution_order],
                                              [i for i, _ in cell_execution_order][1:]))

        # Find skipped cells
        all_code_cells = set(i for i, cell in enumerate(self.notebook.cells)
                             if cell.cell_type == 'code')
        executed_cells = set(i for i, _ in cell_execution_order)
        skipped_cells = all_code_cells - executed_cells

        return {
            "cell_execution_order": cell_execution_order,
            "executed_in_order": in_order,
            "skipped_cells": sorted(list(skipped_cells))
        }

    def generate_pseudo_code(self) -> str:
        """
        Generate pseudo-code from the notebook analysis.

        Returns:
            String containing the pseudo-code
        """
        if not self.notebook:
            self.load_notebook()

        # Analyze the notebook first
        analysis = self.analyze_notebook()

        # Build pseudo-code
        lines = []
        lines.append(f"NOTEBOOK: {os.path.basename(self.notebook_path)}")
        lines.append("=" * 50)

        # General info
        info = analysis["general_info"]
        lines.append(
            f"Total cells: {info['structure']['total_cells']} (Code: {info['structure']['code_cells']}, Markdown: {info['structure']['markdown_cells']})")
        lines.append(f"Kernel: {info['kernel']['name']} ({info['kernel']['language']})")
        lines.append("")

        # Get notebook structure from markdown headers
        lines.append("NOTEBOOK STRUCTURE")
        lines.append("-" * 30)

        for header in analysis["markdown_summary"]["headers"]:
            indent = "  " * (header["level"] - 1)
            lines.append(f"{indent}* {header['text']}")

        lines.append("")

        # Main code workflow
        lines.append("CODE WORKFLOW")
        lines.append("-" * 30)

        workflow = analysis["workflows"]["workflow_stages"]
        for stage, cells in workflow.items():
            if cells:
                lines.append(f"{stage.replace('_', ' ').title()}: Cells {', '.join(map(str, cells))}")

        lines.append("")

        # Framework usage
        lines.append("FRAMEWORKS USED")
        lines.append("-" * 30)

        for category, frameworks in analysis["frameworks"]["categorized"].items():
            if frameworks:
                lines.append(f"{category.replace('_', ' ').title()}: {', '.join(frameworks)}")

        lines.append("")

        # Cell-by-cell pseudocode
        lines.append("CELL-BY-CELL SUMMARY")
        lines.append("-" * 30)

        for i, cell in enumerate(self.notebook.cells):
            if cell.cell_type == 'code' and cell.source.strip():
                lines.append(f"Cell {i} (Code):")

                # Check if cell is in any special category
                categories = []
                for pattern, cells in self.advanced_patterns.items():
                    if i in cells:
                        categories.append(pattern.replace('_', ' ').title())

                if categories:
                    lines.append(f"  Purpose: {', '.join(categories)}")

                # Add simplified code summary
                code_summary = self._generate_cell_pseudo_code(cell.source)
                for line in code_summary:
                    lines.append(f"  {line}")

                lines.append("")

            elif cell.cell_type == 'markdown' and cell.source.strip():
                # For markdown, just extract headers
                md_lines = cell.source.strip().split('\n')
                for line in md_lines:
                    if line.startswith('#'):
                        lines.append(f"Cell {i} (Markdown): {line}")
                        break
                else:
                    # If no header found, use first line
                    first_line = md_lines[0][:50] + ('...' if len(md_lines[0]) > 50 else '')
                    lines.append(f"Cell {i} (Markdown): {first_line}")

                lines.append("")

        # Dependencies
        if analysis["dependencies"]["cell_dependencies"]["edges"]:
            lines.append("CELL DEPENDENCIES")
            lines.append("-" * 30)
            for source, target in analysis["dependencies"]["cell_dependencies"]["edges"]:
                lines.append(f"Cell {source} → Cell {target}")

            lines.append("")

        # Most important entities
        if analysis["dependencies"]["entity_stats"]["most_referenced"]:
            lines.append("KEY ENTITIES")
            lines.append("-" * 30)
            for entity in analysis["dependencies"]["entity_stats"]["most_referenced"]:
                lines.append(f"{entity['name']} ({entity['kind']}): Referenced {entity['references']} times")

            lines.append("")

        # Summary
        lines.append("OVERALL SUMMARY")
        lines.append("-" * 30)

        # Determine notebook type
        notebook_type = "Unknown"
        frameworks = analysis["frameworks"]["framework_usage"]
        patterns = self.advanced_patterns

        if patterns.get('neural_network_definition') or patterns.get('deep_learning_training'):
            if 'tensorflow' in frameworks or 'keras' in frameworks:
                notebook_type = "TensorFlow/Keras Deep Learning"
            elif 'pytorch' in frameworks:
                notebook_type = "PyTorch Deep Learning"
            else:
                notebook_type = "Deep Learning"
        elif patterns.get('model_training'):
            notebook_type = "Machine Learning"
        elif patterns.get('distributed_data_processing'):
            notebook_type = "Big Data Processing"
        elif patterns.get('data_visualization'):
            notebook_type = "Data Visualization"
        elif patterns.get('data_loading') or patterns.get('data_cleaning'):
            notebook_type = "Data Analysis"

        lines.append(f"Notebook Type: {notebook_type}")
        lines.append(f"Workflow Completeness: {analysis['workflows']['workflow_completeness'] * 100:.0f}%")

        workflow_status = "Complete and ordered" if analysis['workflows']['ordered_workflow'] else \
            "Complete but non-linear" if analysis['workflows']['workflow_completeness'] > 0.7 else \
                "Incomplete or exploratory"

        lines.append(f"Workflow Status: {workflow_status}")

        return '\n'.join(lines)

    def _generate_cell_pseudo_code(self, code: str) -> List[str]:
        """Generate pseudo-code for a single cell."""
        lines = []

        try:
            tree = ast.parse(code)

            for node in ast.iter_child_nodes(tree):
                if isinstance(node, ast.Import):
                    names = [name.name for name in node.names]
                    lines.append(f"Import modules: {', '.join(names)}")

                elif isinstance(node, ast.ImportFrom):
                    module = node.module if node.module else ''
                    names = [name.name for name in node.names]
                    lines.append(f"From {module} import: {', '.join(names)}")

                elif isinstance(node, ast.Assign):
                    targets = []
                    for target in node.targets:
                        if isinstance(target, ast.Name):
                            targets.append(target.id)
                        else:
                            targets.append("[complex]")

                    value_desc = self._describe_value(node.value)
                    lines.append(f"Assign {value_desc} to {', '.join(targets)}")

                elif isinstance(node, ast.FunctionDef):
                    args = [arg.arg for arg in node.args.args]
                    lines.append(f"Define function '{node.name}' with {len(args)} parameters")

                elif isinstance(node, ast.ClassDef):
                    bases = []
                    for base in node.bases:
                        if isinstance(base, ast.Name):
                            bases.append(base.id)
                        else:
                            bases.append("[complex]")

                    base_str = f" inheriting from {', '.join(bases)}" if bases else ""
                    lines.append(f"Define class '{node.name}'{base_str}")

                elif isinstance(node, ast.For):
                    if isinstance(node.target, ast.Name):
                        target = node.target.id
                    else:
                        target = "[complex]"

                    lines.append(f"For loop with iterator '{target}'")

                elif isinstance(node, ast.If):
                    lines.append("If condition")

                elif isinstance(node, ast.With):
                    lines.append("With context manager")

                elif isinstance(node, ast.Try):
                    lines.append("Try-except block")

                elif isinstance(node, ast.Expr) and isinstance(node.value, ast.Call):
                    if isinstance(node.value.func, ast.Name):
                        func_name = node.value.func.id
                    elif isinstance(node.value.func, ast.Attribute):
                        if isinstance(node.value.func.value, ast.Name):
                            func_name = f"{node.value.func.value.id}.{node.value.func.attr}"
                        else:
                            func_name = f"[object].{node.value.func.attr}"
                    else:
                        func_name = "[complex]"

                    lines.append(f"Call function {func_name}()")
        except SyntaxError:
            lines.append("[Syntax error in cell]")
        except Exception as e:
            lines.append(f"[Error analyzing cell: {e}]")

        return lines

    def _describe_value(self, node: ast.AST) -> str:
        """Get a description of a value expression."""
        if isinstance(node, ast.Num):
            return f"number {node.n}"
        elif isinstance(node, ast.Str):
            if len(node.s) > 20:
                return f"string '{node.s[:17]}...'"
            return f"string '{node.s}'"
        elif isinstance(node, ast.Name):
            return f"variable '{node.id}'"
        elif isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name):
                return f"result of {node.func.id}()"
            elif isinstance(node.func, ast.Attribute):
                if isinstance(node.func.value, ast.Name):
                    return f"result of {node.func.value.id}.{node.func.attr}()"
                return "result of method call"
            return "result of function call"
        elif isinstance(node, ast.List):
            return "list"
        elif isinstance(node, ast.Dict):
            return "dictionary"
        elif isinstance(node, ast.Tuple):
            return "tuple"
        elif isinstance(node, ast.ListComp):
            return "list comprehension"
        elif isinstance(node, ast.BinOp):
            return "arithmetic expression"
        else:
            return "complex expression"

    def visualize_cell_dependencies(self) -> None:
        """Visualize cell dependencies as a graph."""
        if not self.cell_dependencies:
            print("No cell dependencies found.")
            return

        G = self.cell_dependencies.copy()

        # Set node colors based on cell clusters
        node_colors = []
        labels = {}

        clusters = self._cluster_cells()
        cell_to_cluster = {}
        for cluster, cells in clusters.items():
            for cell in cells:
                cell_to_cluster[cell] = cluster

        color_map = {
            "imports": "lightblue",
            "data_loading": "green",
            "data_preparation": "yellow",
            "modeling": "red",
            "visualization": "purple",
            "utility_functions": "orange",
            "output_display": "gray",
            "miscellaneous": "white"
        }

        for node in G.nodes():
            cluster = cell_to_cluster.get(node, "miscellaneous")
            labels[node] = f"Cell {node}\n({cluster})"
            node_colors.append(color_map.get(cluster, "white"))

        plt.figure(figsize=(12, 8))
        pos = nx.spring_layout(G, seed=42)
        nx.draw_networkx_nodes(G, pos, node_color=node_colors, node_size=500, alpha=0.8)
        nx.draw_networkx_edges(G, pos, edge_color='gray', arrows=True)
        nx.draw_networkx_labels(G, pos, labels=labels, font_size=8)

        plt.title("Cell Dependencies")
        plt.axis('off')
        plt.tight_layout()
        plt.savefig("cell_dependencies.png")
        plt.close()

        print("Cell dependency graph saved as 'cell_dependencies.png'")

    def visualize_workflow(self) -> None:
        """Visualize the data science workflow in the notebook."""
        # Analyze the notebook
        analysis = self.analyze_notebook()
        workflow = analysis["workflows"]["workflow_stages"]

        # Create workflow graph
        G = nx.DiGraph()

        stages = ["data_loading", "data_exploration", "data_cleaning",
                  "feature_engineering", "model_building", "model_evaluation",
                  "model_deployment"]

        # Add nodes for each stage
        for i, stage in enumerate(stages):
            G.add_node(stage, pos=(i, 0))

        # Add edges between consecutive stages
        for i in range(len(stages) - 1):
            G.add_edge(stages[i], stages[i + 1])

        # Set node colors based on presence
        node_colors = []
        for stage in stages:
            if workflow[stage]:
                node_colors.append('green')
            else:
                node_colors.append('lightgray')

        # Draw the graph
        plt.figure(figsize=(12, 4))
        pos = nx.get_node_attributes(G, 'pos')
        nx.draw_networkx_nodes(G, pos, node_color=node_colors, node_size=2000, alpha=0.8)
        nx.draw_networkx_edges(G, pos, edge_color='gray', arrows=True)

        # Add labels with nicer formatting
        labels = {stage: stage.replace('_', ' ').title() for stage in stages}
        nx.draw_networkx_labels(G, pos, labels=labels)

        plt.title("Data Science Workflow")
        plt.axis('off')
        plt.tight_layout()
        plt.savefig("workflow.png")
        plt.close()

        print("Workflow visualization saved as 'workflow.png'")

    def generate_html_report(self) -> str:
        """Generate an HTML report of the notebook analysis."""
        analysis = self.analyze_notebook()

        html = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Notebook Analysis Report</title>
        </head>
        <body style="font-family: Arial, sans-serif; margin: 20px;">
            <h1 style="color: #333;">Notebook Analysis Report: {}</h1>
        """.format(os.path.basename(self.notebook_path))

        # General Info
        html += """
            <div style="margin-bottom: 20px;">
                <h2 style="color: #333;">General Information</h2>
                <div style="border: 1px solid #ddd; border-radius: 5px; padding: 15px; margin-bottom: 10px;">
                    <p><b>Total Cells:</b> {total} (Code: {code}, Markdown: {markdown})</p>
                    <p><b>Kernel:</b> {kernel} ({language})</p>
                </div>
            </div>
        """.format(
            total=analysis["general_info"]["structure"]["total_cells"],
            code=analysis["general_info"]["structure"]["code_cells"],
            markdown=analysis["general_info"]["structure"]["markdown_cells"],
            kernel=analysis["general_info"]["kernel"]["name"],
            language=analysis["general_info"]["kernel"]["language"]
        )

        # Frameworks
        html += """
            <div class="section">
                <h2>Frameworks Used</h2>
                <div class="card">
        """

        for category, frameworks in analysis["frameworks"]["categorized"].items():
            if frameworks:
                html += "<p><b>{}</b>: {}</p>".format(
                    category.replace('_', ' ').title(),
                    ', '.join('<span class="tag">{}</span>'.format(fw) for fw in frameworks)
                )

        html += """
                </div>
            </div>
        """

        # Workflow Analysis
        html += """
            <div class="section">
                <h2>Workflow Analysis</h2>
                <div class="card">
                    <p><b>Workflow Completeness:</b> {completeness:.0f}%</p>
                    <p><b>Workflow Status:</b> {status}</p>
        """.format(
            completeness=analysis["workflows"]["workflow_completeness"] * 100,
            status="Complete and ordered" if analysis["workflows"]["ordered_workflow"] else
            "Complete but non-linear" if analysis["workflows"]["workflow_completeness"] > 0.7 else
            "Incomplete or exploratory"
        )

        html += "<p><b>Workflow Stages:</b></p><ul>"
        for stage, cells in analysis["workflows"]["workflow_stages"].items():
            if cells:
                html += "<li>{}: Cells {}</li>".format(
                    stage.replace('_', ' ').title(),
                    ', '.join(map(str, cells))
                )
        html += "</ul>"
        html += """
                </div>
            </div>
        """

        # Cell clustering
        html += """
            <div class="section">
                <h2>Cell Clusters</h2>
                <div class="card">
        """

        clusters = analysis["cell_clusters"]
        for cluster, cells in clusters.items():
            html += "<p><b>{}</b>: Cells {}</p>".format(
                cluster.replace('_', ' ').title(),
                ', '.join(map(str, cells))
            )

        html += """
                </div>
            </div>
        """

        # Key entities
        if analysis["dependencies"]["entity_stats"]["most_referenced"]:
            html += """
                <div class="section">
                    <h2>Key Entities</h2>
                    <div class="card">
                        <ul>
            """

            for entity in analysis["dependencies"]["entity_stats"]["most_referenced"]:
                html += "<li><b>{}</b> ({}): Referenced {} times</li>".format(
                    entity["name"], entity["kind"], entity["references"]
                )

            html += """
                        </ul>
                    </div>
                </div>
            """

        # Final HTML
        html += """
            <div class="section">
                <h2>Code Patterns Detected</h2>
                <div class="card">
        """

        for pattern, cells in self.advanced_patterns.items():
            if cells:
                html += "<p><b>{}</b>: Cells {}</p>".format(
                    pattern.replace('_', ' ').title(),
                    ', '.join(map(str, cells))
                )

        html += """
                </div>
            </div>
        """

        html += """
        </body>
        </html>
        """

        return html


def analyze_jupyter_notebook(notebook_path: str,
                             generate_visualizations: bool = False,
                             output_format: str = 'text') -> str:
    """
    Analyze a Jupyter notebook and generate pseudo code or reports.

    Args:
        notebook_path: Path to the Jupyter notebook file
        generate_visualizations: Whether to generate visualizations
        output_format: Format of the output ('text', 'html')

    Returns:
        Analysis results in the specified format
    """
    analyzer = AdvancedNotebookAnalyzer(notebook_path)

    if generate_visualizations:
        analyzer.visualize_cell_dependencies()
        analyzer.visualize_workflow()

    if output_format == 'html':
        return analyzer.generate_html_report()
    else:
        return analyzer.generate_pseudo_code()


if __name__ == "__main__":
    """
    # Basic usage
    python advanced_notebook_analyzer.py /path/to/your_notebook.ipynb
    
    # Generate visualizations
    python advanced_notebook_analyzer.py /path/to/your_notebook.ipynb --visualize
    
    # Generate HTML report
    python advanced_notebook_analyzer.py /path/to/your_notebook.ipynb --output html --save report.html
    """
    import argparse

    parser = argparse.ArgumentParser(description='Analyze Jupyter notebooks and generate pseudo code')
    parser.add_argument('notebook_path', help='Path to the Jupyter notebook file')
    parser.add_argument('--visualize', '-v', action='store_true', help='Generate visualizations')
    parser.add_argument('--output', '-o', choices=['text', 'html'], default='text',
                        help='Output format (text or html)')
    parser.add_argument('--save', '-s', help='Save output to file')

    args = parser.parse_args()

    result = analyze_jupyter_notebook(args.notebook_path, args.visualize, args.output)

    if args.save:
        with open(args.save, 'w', encoding='utf-8') as f:
            f.write(result)
        print(f"Analysis saved to {args.save}")
    else:
        print(result)