import nbformat
import ast
import re
from typing import List, Dict, Any, Optional, Union, Tuple


class JupyterAnalyzer:
    """
    A class to analyze Jupyter notebook code and generate pseudo code.
    """

    def __init__(self, notebook_path: str):
        """
        Initialize the analyzer with a notebook path.

        Args:
            notebook_path: Path to the Jupyter notebook file
        """
        self.notebook_path = notebook_path
        self.notebook = None
        self.pseudo_code = []

    def load_notebook(self) -> None:
        """Load the Jupyter notebook."""
        try:
            with open(self.notebook_path, 'r', encoding='utf-8') as f:
                self.notebook = nbformat.read(f, as_version=4)
            print(f"Successfully loaded notebook: {self.notebook_path}")
        except Exception as e:
            print(f"Error loading notebook: {e}")
            raise

    def _analyze_import(self, node: ast.Import) -> str:
        """Analyze an import statement."""
        return f"Import module(s): {', '.join(name.name for name in node.names)}"

    def _analyze_import_from(self, node: ast.ImportFrom) -> str:
        """Analyze an import from statement."""
        module = node.module if node.module else ''
        return f"Import {', '.join(name.name for name in node.names)} from module {module}"

    def _analyze_assign(self, node: ast.Assign) -> str:
        """Analyze an assignment statement."""
        targets = []
        for target in node.targets:
            if isinstance(target, ast.Name):
                targets.append(target.id)
            elif isinstance(target, ast.Tuple) or isinstance(target, ast.List):
                elements = []
                for elt in target.elts:
                    if isinstance(elt, ast.Name):
                        elements.append(elt.id)
                    else:
                        elements.append("complex_expression")
                targets.append(f"({', '.join(elements)})")
            else:
                targets.append("complex_expression")

        # Basic value analysis
        value_str = self._get_value_description(node.value)
        return f"Assign {value_str} to {', '.join(targets)}"

    def _get_value_description(self, node: ast.AST) -> str:
        """Get a description of an expression."""
        if isinstance(node, ast.Num):
            return f"number {node.n}"
        elif isinstance(node, ast.Str):
            return f"string '{node.s[:10]}...' " if len(node.s) > 10 else f"string '{node.s}'"
        elif isinstance(node, ast.Name):
            return f"variable {node.id}"
        elif isinstance(node, ast.Call):
            func_name = ""
            if isinstance(node.func, ast.Name):
                func_name = node.func.id
            elif isinstance(node.func, ast.Attribute):
                if isinstance(node.func.value, ast.Name):
                    func_name = f"{node.func.value.id}.{node.func.attr}"
                else:
                    func_name = f"object.{node.func.attr}"
            return f"result of function call {func_name}()"
        elif isinstance(node, ast.BinOp):
            return "result of binary operation"
        elif isinstance(node, ast.List):
            return "list"
        elif isinstance(node, ast.Dict):
            return "dictionary"
        elif isinstance(node, ast.Tuple):
            return "tuple"
        elif isinstance(node, ast.Set):
            return "set"
        elif isinstance(node, ast.ListComp):
            return "list comprehension"
        elif isinstance(node, ast.DictComp):
            return "dict comprehension"
        elif isinstance(node, ast.SetComp):
            return "set comprehension"
        else:
            return "complex expression"

    def _analyze_function_def(self, node: ast.FunctionDef) -> List[str]:
        """Analyze a function definition."""
        lines = []

        # Function parameters
        params = []
        for arg in node.args.args:
            params.append(arg.arg)

        lines.append(f"Define function '{node.name}' with parameters ({', '.join(params)})")

        # Function docstring
        if (node.body and isinstance(node.body[0], ast.Expr) and
                isinstance(node.body[0].value, ast.Str)):
            doc = node.body[0].value.s
            doc_summary = doc.split('\n')[0].strip()
            lines.append(f"  Documentation: {doc_summary}")

        # Function body analysis (simplified)
        lines.append(f"  Function body has {len(node.body)} statement(s)")

        return lines

    def _analyze_class_def(self, node: ast.ClassDef) -> List[str]:
        """Analyze a class definition."""
        lines = []

        # Class inheritance
        bases = []
        for base in node.bases:
            if isinstance(base, ast.Name):
                bases.append(base.id)
            else:
                bases.append("complex_base")

        if bases:
            lines.append(f"Define class '{node.name}' inheriting from ({', '.join(bases)})")
        else:
            lines.append(f"Define class '{node.name}'")

        # Class docstring
        if (node.body and isinstance(node.body[0], ast.Expr) and
                isinstance(node.body[0].value, ast.Str)):
            doc = node.body[0].value.s
            doc_summary = doc.split('\n')[0].strip()
            lines.append(f"  Documentation: {doc_summary}")

        # Methods count
        methods = [b for b in node.body if isinstance(b, ast.FunctionDef)]
        lines.append(f"  Class has {len(methods)} method(s)")

        return lines

    def _analyze_for_loop(self, node: ast.For) -> str:
        """Analyze a for loop."""
        iter_desc = self._get_value_description(node.iter)
        if isinstance(node.target, ast.Name):
            target = node.target.id
        else:
            target = "complex_target"

        return f"Loop through {iter_desc} as {target}"

    def _analyze_if_statement(self, node: ast.If) -> str:
        """Analyze an if statement."""
        if isinstance(node.test, ast.Compare):
            if isinstance(node.test.left, ast.Name):
                left = node.test.left.id
            else:
                left = "expression"

            if len(node.test.comparators) > 0:
                if isinstance(node.test.comparators[0], ast.Name):
                    right = node.test.comparators[0].id
                else:
                    right = "expression"

                op = type(node.test.ops[0]).__name__
                return f"If condition: {left} {op} {right}"

        return "If condition"

    def _analyze_while_loop(self, node: ast.While) -> str:
        """Analyze a while loop."""
        return "While loop with condition"

    def _analyze_try_except(self, node: ast.Try) -> List[str]:
        """Analyze a try/except block."""
        lines = []
        lines.append("Try block")

        for handler in node.handlers:
            if handler.type:
                if isinstance(handler.type, ast.Name):
                    except_type = handler.type.id
                else:
                    except_type = "complex_exception"
                lines.append(f"  Except block handling {except_type}")
            else:
                lines.append("  Except block handling all exceptions")

        if node.finalbody:
            lines.append("  Finally block")

        return lines

    def _analyze_with_statement(self, node: ast.With) -> str:
        """Analyze a with statement."""
        contexts = []
        for item in node.items:
            if isinstance(item.context_expr, ast.Call):
                if isinstance(item.context_expr.func, ast.Name):
                    contexts.append(item.context_expr.func.id)
                elif isinstance(item.context_expr.func, ast.Attribute):
                    if isinstance(item.context_expr.func.value, ast.Name):
                        contexts.append(f"{item.context_expr.func.value.id}.{item.context_expr.func.attr}")
                    else:
                        contexts.append(f"object.{item.context_expr.func.attr}")
                else:
                    contexts.append("complex_context")
            else:
                contexts.append("context")

        return f"With context manager: {', '.join(contexts)}"

    def _analyze_return(self, node: ast.Return) -> str:
        """Analyze a return statement."""
        if node.value:
            value_desc = self._get_value_description(node.value)
            return f"Return {value_desc}"
        else:
            return "Return None"

    def _analyze_call(self, node: ast.Call) -> str:
        """Analyze a function call."""
        func_name = ""
        if isinstance(node.func, ast.Name):
            func_name = node.func.id
        elif isinstance(node.func, ast.Attribute):
            if isinstance(node.func.value, ast.Name):
                func_name = f"{node.func.value.id}.{node.func.attr}"
            else:
                func_name = f"object.{node.func.attr}"

        arg_count = len(node.args) + len(node.keywords)
        return f"Call function {func_name}() with {arg_count} argument(s)"

    def _analyze_code_cell(self, cell_code: str, cell_num: int) -> List[str]:
        """
        Analyze a code cell and return pseudo code lines.

        Args:
            cell_code: The source code in the cell
            cell_num: The cell number

        Returns:
            List of pseudo code lines
        """
        lines = []
        lines.append(f"--- Cell {cell_num} ---")

        # Look for framework-specific patterns before AST parsing
        framework_patterns = self._detect_framework_patterns(cell_code)
        if framework_patterns:
            lines.extend(framework_patterns)

        try:
            tree = ast.parse(cell_code)

            for node in ast.iter_child_nodes(tree):
                if isinstance(node, ast.Import):
                    lines.append(self._analyze_import(node))
                elif isinstance(node, ast.ImportFrom):
                    lines.append(self._analyze_import_from(node))
                elif isinstance(node, ast.Assign):
                    lines.append(self._analyze_assign(node))
                elif isinstance(node, ast.FunctionDef):
                    lines.extend(self._analyze_function_def(node))
                elif isinstance(node, ast.ClassDef):
                    lines.extend(self._analyze_class_def(node))
                elif isinstance(node, ast.For):
                    lines.append(self._analyze_for_loop(node))
                elif isinstance(node, ast.If):
                    lines.append(self._analyze_if_statement(node))
                elif isinstance(node, ast.While):
                    lines.append(self._analyze_while_loop(node))
                elif isinstance(node, ast.Try):
                    lines.extend(self._analyze_try_except(node))
                elif isinstance(node, ast.With):
                    lines.append(self._analyze_with_statement(node))
                elif isinstance(node, ast.Return):
                    lines.append(self._analyze_return(node))
                elif isinstance(node, ast.Expr) and isinstance(node.value, ast.Call):
                    lines.append(self._analyze_call(node.value))
                elif isinstance(node, ast.Expr) and not isinstance(node.value, ast.Str):
                    lines.append("Expression statement")
        except SyntaxError as e:
            lines.append(f"[Syntax error in this cell]: {e}")
        except Exception as e:
            lines.append(f"[Error analyzing cell]: {e}")

        if len(lines) == 1:  # Only has the cell header
            lines.append("[Empty cell or only comments/markdown]")

    def _detect_framework_patterns(self, code: str) -> List[str]:
        """
        Detect framework-specific patterns in code.

        Args:
            code: The source code

        Returns:
            List of descriptions for detected patterns
        """
        patterns = []

        # PySpark patterns
        if re.search(r'SparkSession\.builder', code):
            patterns.append("Initialize Spark session")
        if re.search(r'createDataFrame', code):
            patterns.append("Create Spark DataFrame")
        if re.search(r'spark\.read\.(parquet|csv|json|text|orc)', code):
            patterns.append("Load data into Spark DataFrame")
        if re.search(r'withColumn|select|filter|groupBy|agg|join|union', code):
            patterns.append("Perform Spark DataFrame transformations")
        if re.search(r'\.rdd\.|parallelize|mapPartitions|reduceByKey', code):
            patterns.append("Perform low-level Spark RDD operations")
        if re.search(r'write\.(parquet|csv|json|text|orc)', code):
            patterns.append("Save Spark DataFrame to storage")

        # TensorFlow patterns
        if re.search(r'tf\.keras\.Sequential|tf\.keras\.Model', code):
            patterns.append("Define TensorFlow/Keras neural network model")
        if re.search(r'tf\.data\.Dataset', code):
            patterns.append("Create TensorFlow data pipeline")
        if re.search(r'\.compile\(.*loss.*optimizer', code):
            patterns.append("Configure TensorFlow model training parameters")
        if re.search(r'\.fit\(', code) and re.search(r'tf\.|keras', code):
            patterns.append("Train TensorFlow model")
        if re.search(r'tf\.saved_model\.save|model\.save', code):
            patterns.append("Save TensorFlow model")

        # PyTorch patterns
        if re.search(r'class.*\(nn\.Module\)', code) or re.search(r'torch\.nn\.Module', code):
            patterns.append("Define PyTorch neural network model")
        if re.search(r'torch\.utils\.data\.DataLoader', code):
            patterns.append("Create PyTorch data loader")
        if re.search(r'optimizer\.zero_grad|loss\.backward\(\)|optimizer\.step\(\)', code):
            patterns.append("Implement PyTorch training loop")
        if re.search(r'torch\.save', code):
            patterns.append("Save PyTorch model")

        # Distributed computing frameworks
        if re.search(r'dask\.(dataframe|array|delayed)', code):
            patterns.append("Use Dask for parallel computing")
        if re.search(r'ray\.init|@ray\.remote', code):
            patterns.append("Use Ray for distributed computing")

        # NLP frameworks
        if re.search(r'transformers\.(AutoModel|AutoTokenizer|pipeline)', code):
            patterns.append("Use Hugging Face Transformers for NLP tasks")

        return patterns

        return lines

    def identify_data_transformations(self, code: str) -> Dict[str, Any]:
        """
        Identify data transformation operations in the code.

        Args:
            code: The code to analyze

        Returns:
            Dictionary with transformation information
        """
        transformations = {
            "data_loading": False,
            "data_cleaning": False,
            "feature_engineering": False,
            "visualization": False,
            "modeling": False,
            "evaluation": False,
            "distributed_computing": False,
            "deep_learning": False,
            "hyperparameter_tuning": False,
            "nlp_processing": False,
            "image_processing": False
        }

        # Data loading patterns
        if re.search(
                r'read_csv|load_data|open\(|pd\.read_|read_excel|load_dataset|read\.parquet|read\.json|createDataFrame',
                code):
            transformations["data_loading"] = True

        # Data cleaning patterns
        if re.search(r'dropna|fillna|replace|clean|drop_duplicates|isnull|isna|withColumn|dropDuplicates|filter', code):
            transformations["data_cleaning"] = True

        # Feature engineering patterns
        if re.search(
                r'apply|map|transform|get_dummies|OneHotEncoder|StandardScaler|feature|udf|withColumn|select|StringIndexer|VectorAssembler',
                code):
            transformations["feature_engineering"] = True

        # Visualization patterns
        if re.search(r'plot|plt\.|matplotlib|seaborn|sns\.|figure|hist|bar|scatter|boxplot|tensorboard', code):
            transformations["visualization"] = True

        # Modeling patterns
        if re.search(
                r'fit|predict|train_test_split|RandomForest|LinearRegression|LogisticRegression|model|Pipeline|CrossValidator|RandomForestClassifier|GBTClassifier',
                code):
            transformations["modeling"] = True

        # Evaluation patterns
        if re.search(
                r'accuracy_score|precision_score|recall|f1_score|mean_squared_error|confusion_matrix|evaluate|BinaryClassificationEvaluator|RegressionEvaluator',
                code):
            transformations["evaluation"] = True

        # Distributed computing patterns
        if re.search(
                r'spark|SparkSession|SparkContext|sc\.|createDataFrame|dask|parallelize|cluster|executors|workers|driver',
                code):
            transformations["distributed_computing"] = True

        # Deep learning patterns
        if re.search(
                r'keras|Sequential|Dense|Conv2D|LSTM|GRU|Embedding|Dropout|BatchNormalization|Activation|tf\.|torch\.|nn\.|Module|backward|optim|loss',
                code):
            transformations["deep_learning"] = True

        # Hyperparameter tuning patterns
        if re.search(
                r'GridSearchCV|RandomizedSearchCV|BayesianOptimization|optuna|hyperopt|hyperparameter|ParamGridBuilder|CrossValidator',
                code):
            transformations["hyperparameter_tuning"] = True

        # NLP processing patterns
        if re.search(
                r'tokenize|corpus|CountVectorizer|TfidfVectorizer|word_tokenize|nltk|spacy|transformers|tokenizer|bert|gpt|llm|embedding|token|vocab',
                code):
            transformations["nlp_processing"] = True

        # Image processing patterns
        if re.search(
                r'cv2\.|image|img|imread|imshow|resize|rotate|augmentation|PIL|Image\.open|ImageDataGenerator|transforms',
                code):
            transformations["image_processing"] = True

        return transformations

    def detect_libraries(self, code: str) -> List[str]:
        """
        Detect libraries used in the code.

        Args:
            code: The code to analyze

        Returns:
            List of detected libraries
        """
        libraries = set()

        # Common data science libraries
        lib_patterns = {
            "pandas": r'pd\.|pandas',
            "numpy": r'np\.|numpy',
            "matplotlib": r'plt\.|matplotlib',
            "seaborn": r'sns\.|seaborn',
            "scikit-learn": r'sklearn',
            "tensorflow": r'tf\.|tensorflow',
            "pytorch": r'torch',
            "keras": r'keras',
            "scipy": r'scipy',
            "pyspark": r'spark|SparkSession|SparkContext|pyspark',
            "dask": r'dask',
            "xgboost": r'xgb|xgboost',
            "lightgbm": r'lgb|lightgbm',
            "nltk": r'nltk',
            "spacy": r'spacy',
            "huggingface": r'transformers|tokenizers|datasets',
            "beautifulsoup": r'bs4|BeautifulSoup',
            "requests": r'requests\.',
            "opencv": r'cv2\.',
            "polars": r'pl\.|polars',
            "fastai": r'fastai',
            "jax": r'jax',
            "cupy": r'cupy|cp\.',
            "mlflow": r'mlflow',
            "ray": r'ray',
        }

        for lib, pattern in lib_patterns.items():
            if re.search(pattern, code):
                libraries.add(lib)

        return sorted(list(libraries))

    def analyze_notebook(self) -> str:
        """
        Analyze the notebook and generate pseudo code.

        Returns:
            Pseudo code as a string
        """
        if not self.notebook:
            self.load_notebook()

        self.pseudo_code = []
        self.pseudo_code.append(f"NOTEBOOK ANALYSIS: {self.notebook_path}")
        self.pseudo_code.append("=" * 50)

        # Initialize counters
        total_cells = len(self.notebook.cells)
        code_cells = sum(1 for cell in self.notebook.cells if cell.cell_type == 'code')
        markdown_cells = sum(1 for cell in self.notebook.cells if cell.cell_type == 'markdown')

        self.pseudo_code.append(f"Total cells: {total_cells} (Code: {code_cells}, Markdown: {markdown_cells})")
        self.pseudo_code.append("")

        # Initialize aggregated libraries and transformations
        all_libraries = set()
        all_transformations = {
            "data_loading": False,
            "data_cleaning": False,
            "feature_engineering": False,
            "visualization": False,
            "modeling": False,
            "evaluation": False
        }

        # Process each cell
        for i, cell in enumerate(self.notebook.cells):
            if cell.cell_type == 'code' and cell.source.strip():
                # Analyze code
                cell_analysis = self._analyze_code_cell(cell.source, i + 1)
                self.pseudo_code.extend(cell_analysis)

                # Detect libraries
                libs = self.detect_libraries(cell.source)
                all_libraries.update(libs)

                # Identify transformations
                transforms = self.identify_data_transformations(cell.source)
                for key, value in transforms.items():
                    if value:
                        all_transformations[key] = True

                self.pseudo_code.append("")
            elif cell.cell_type == 'markdown' and cell.source.strip():
                # Extract the first line or first few words of markdown
                md_lines = cell.source.strip().split('\n')
                md_summary = md_lines[0][:50] + ('...' if len(md_lines[0]) > 50 else '')
                if '# ' in md_summary:
                    md_summary = f"SECTION: {md_summary.replace('# ', '')}"

                self.pseudo_code.append(f"--- Cell {i + 1} [Markdown] ---")
                self.pseudo_code.append(md_summary)
                self.pseudo_code.append("")

        # Add summary at the end
        self.pseudo_code.append("=" * 50)
        self.pseudo_code.append("NOTEBOOK SUMMARY")
        self.pseudo_code.append("=" * 50)
        self.pseudo_code.append(f"Libraries used: {', '.join(sorted(all_libraries))}")

        # List transformations that were detected
        transformations_found = [k.replace('_', ' ').title() for k, v in all_transformations.items() if v]
        if transformations_found:
            self.pseudo_code.append(f"Operations detected: {', '.join(transformations_found)}")

        # Add framework-specific insights
        self._add_framework_insights(all_libraries)

        return '\n'.join(self.pseudo_code)

    def _add_framework_insights(self, libraries):
        """
        Add framework-specific insights to the analysis.

        Args:
            libraries: Set of detected libraries
        """
        framework_insights = []

        # Check for big data frameworks
        big_data_frameworks = {'pyspark', 'dask', 'ray'}
        big_data_libs = big_data_frameworks.intersection(libraries)
        if big_data_libs:
            framework_insights.append(
                f"Big Data Processing: Using {', '.join(big_data_libs)} for distributed data processing")

        # Check for deep learning frameworks
        dl_frameworks = {'tensorflow', 'pytorch', 'keras', 'fastai', 'jax'}
        dl_libs = dl_frameworks.intersection(libraries)
        if dl_libs:
            framework_insights.append(f"Deep Learning: Using {', '.join(dl_libs)} for neural network modeling")

        # Check for traditional ML frameworks
        ml_frameworks = {'scikit-learn', 'xgboost', 'lightgbm'}
        ml_libs = ml_frameworks.intersection(libraries)
        if ml_libs:
            framework_insights.append(f"Machine Learning: Using {', '.join(ml_libs)} for traditional modeling")

        # Check for NLP frameworks
        nlp_frameworks = {'nltk', 'spacy', 'huggingface'}
        nlp_libs = nlp_frameworks.intersection(libraries)
        if nlp_libs:
            framework_insights.append(f"Natural Language Processing: Using {', '.join(nlp_libs)} for text analysis")

        # Check for visualization frameworks
        viz_frameworks = {'matplotlib', 'seaborn'}
        viz_libs = viz_frameworks.intersection(libraries)
        if viz_libs:
            framework_insights.append(f"Data Visualization: Using {', '.join(viz_libs)} for plotting")

        # Add insights to pseudo code
        if framework_insights:
            self.pseudo_code.append("\nFRAMEWORK INSIGHTS:")
            self.pseudo_code.extend(framework_insights)


def analyze_notebook(notebook_path: str) -> str:
    """
    Analyze a Jupyter notebook and generate pseudo code.

    Args:
        notebook_path: Path to the Jupyter notebook file

    Returns:
        Generated pseudo code as a string
    """
    analyzer = JupyterAnalyzer(notebook_path)
    return analyzer.analyze_notebook()


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        # python notebook_analyzer.py my_notebook.ipynb
        print("Usage: python notebook_analyzer.py [notebook_path]")
        sys.exit(1)

    notebook_path = sys.argv[1]
    result = analyze_notebook(notebook_path)
    print(result)