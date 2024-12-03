# api_gateway/llm_service.py
from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline
from .prompt_service import generate_prompt
from .sql_service import extract_sql_query
import torch
import logging
import sqlparse



logger = logging.getLogger(__name__)

MODEL_NAME = "defog/sqlcoder-7b-2"
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)

available_memory = torch.cuda.get_device_properties(0).total_memory


if available_memory > 15e9:
    # if you have atleast 15GB of GPU memory, run load the model in float16
    model = AutoModelForCausalLM.from_pretrained(
        MODEL_NAME,
        trust_remote_code=True,
        torch_dtype=torch.float16,
        device_map="auto",
        use_cache=True,
    )
else:
    # else, load in 8 bits – this is a bit slower
    model = AutoModelForCausalLM.from_pretrained(
        MODEL_NAME,
        trust_remote_code=True,
        # torch_dtype=torch.float16,
        load_in_8bit=True,
        device_map="auto",
        use_cache=True,
    )



def generate_sql_from_nl(nl_query, schema_info):
    """
    Generate SQL query from natural language query using defog/sqlcoder-7b-2.
    
    Args:
        nl_query (str): User's query in natural language.
        schema_info (dict): Filtered schema information.

    Returns:
        str: Generated SQL query.
    """
    try:
        prompt = generate_prompt(nl_query, schema_info)
        inputs = tokenizer(prompt, return_tensors="pt").to("cuda")

        generated_ids = model.generate(
            **inputs,
            num_return_sequences=1,
            eos_token_id=tokenizer.eos_token_id,
            pad_token_id=tokenizer.eos_token_id,
            max_new_tokens=400,
            do_sample=False,
            num_beams=1,
        )

        outputs = tokenizer.batch_decode(generated_ids, skip_special_tokens=True)
        
        # Extract the SQL query from the output
        full_output = outputs[0].strip()
        sql_query = extract_sql_query(full_output)  # Function to cleanly extract SQL
        
        print(f" \n Generated SQL Query: {sql_query}")




        #validated_query = validate_and_correct_columns(sql_query, schema_info)

        
        #print(f" \n Validated SQL Query: {sql_query}")


        torch.cuda.empty_cache()
        torch.cuda.synchronize()




        return sqlparse.format(sql_query, reindent=True)
    

    except Exception as e:
        print(f"Error generating SQL: {e}")
        return None

