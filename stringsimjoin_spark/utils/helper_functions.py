
def get_output_header_from_tables(candset_key_attr,
                                  l_key_attr, r_key_attr,
                                  l_out_attrs, r_out_attrs,
                                  l_out_prefix, r_out_prefix):
    output_header = []

    output_header.append(candset_key_attr)

    output_header.append(l_out_prefix + l_key_attr)

    if l_out_attrs:
        for l_attr in l_out_attrs:
            output_header.append(l_out_prefix + l_attr)

    output_header.append(r_out_prefix + r_key_attr)

    if r_out_attrs:
        for r_attr in r_out_attrs:
            output_header.append(r_out_prefix + r_attr)

    return output_header
