# Converting Python Scripts into Apache NiFi 2.x Processors

This guide explains how to convert a standalone Python script into a native Apache NiFi 2.x processor using our team template (`CustomScriptProcessor.py`).

## To take into account

* **Configuration Priority (Processor > FlowFile):** Every parameter checks the NiFi Processor Properties first. If left blank in the processor UI, it automatically falls back to reading the matching attribute from the incoming FlowFile. If neither exists, the FlowFile is routed to `failure`.

---

## Step-by-Step Conversion Guide (`TODO 1` – `TODO 13`)

Copy the template file and work through the `TODO` markers in order:

### 1. Imports & Local Helper Files (`TODO 1` – `TODO 2`)
* **`TODO 1`:** Add standard library and third-party imports needed by your script at the top of the file. *(Note: If a third-party package causes issues during NiFi processor creation, move its `import` statement inside `transform()` or the function that uses it.)*
* **`TODO 2`:** Keep the `sys.path.insert(...)` block if your script imports helper functions from another `.py` file stored in the same directory as the processor. Otherwise, you can remove it.

### 2. Processor Identity & Dependencies (`TODO 3` – `TODO 4`)
* **`TODO 3`:** Rename `CustomScriptProcessor` to match your new processor's name (e.g., `CreateConfusionMatrices`). **The `.py` filename must match the class name** (`CreateConfusionMatrices.py`). Also update the class name reference inside `transform()` (`TODO 11`).
* **`TODO 4`:** Update `ProcessorDetails`:
  * `version`: Use versioning (e.g., `'0.0.1'`).
  * `description`: Briefly explain what the processor does (shown in the NiFi UI when adding a processor).
  * `tags`: Keywords teammates can search for in the NiFi UI to find the processor.
  * `dependencies`: List any PyPI packages required (e.g., `["pandas==2.2.2", "requests>=2.31.0"]`). NiFi 2.x automatically creates an isolated virtual environment and installs these.

### 3. Relationships & Parameters (`TODO 5` – `TODO 7`)
* **`TODO 5`:** Update the `description` strings for `SUCCESS` and `FAILURE` so users know what routes to each relationship on the canvas.
* **`TODO 6`:** Create one `PropertyDescriptor` code block in `__init__` for every input parameter your script needs. *Keep `required=False` if you want users to be able to pass the value via a FlowFile attribute instead of hardcoding it in the processor.*
* **`TODO 7`:** Register every `PropertyDescriptor` you created inside `self.descriptors = [...]`. Any property omitted from this list will not appear in the NiFi UI.

### 4. Script Functions (`TODO 8`)
* **`TODO 8`:** Paste your script's functions inside the processor class.
  * Put `@staticmethod` above every function that does **not** need access to `self` (such as `self.logger`).
  * Omit `@staticmethod` and include `self` as the first argument only if the function needs to log via `self.logger` or read variables stored on `self`.
  * Remember that values coming from NiFi properties and FlowFile attributes are always **strings (`str`)**—cast them (`int()`, `float()`, `json.loads()`) inside your function if needed.

### 5. Execution, Content & Error Handling (`TODO 9` – `TODO 13`)
* **`TODO 9`:** Replace `xxx` in `self.logger.info(...)` with your processor's name or action to mark execution start in `nifi-app.log`.
* **`TODO 10`:** Duplicate the parameter-loading block for each variable defined in `TODO 6`:
  ```python
  my_param = context.getProperty(self.my_param).getValue()
  if my_param is None or not my_param.strip():
      my_param = flowfile.getAttribute("my_param")
  if my_param is None or not my_param.strip():
      self.logger.error("my_param missing from processor property and FlowFile attribute 'my_param'")
      return FlowFileTransformResult(relationship="failure")
* **`TODO 11`:** Call your main script function using the renamed class (for example, `MyProcessorName.main_function(...)`)
    * If your script reads the FlowFile's payload: Read it before calling your function, using:
    ```python
    input_content = flowfile.getContentsAsBytes().decode("utf-8")
* **`TODO 12`:** Return your results in `FlowFileTransformResult`:
    * Use `attributes={...}` to pass string values downstream to subsequent NiFi procesors through the FlowFile.

* **`TODO 13`:** Update the error log message to describe what failed