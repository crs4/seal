for test_name in t001 t002; do
    for tag in read mate errors; do
	gunzip -c ${test_name}_${tag}.gz >${test_name}_${tag}
    done
done
